package hydra.ingest.http

import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit._
import cats.data.NonEmptyList
import cats.effect.{Concurrent, ContextShift, IO, Sync, Timer}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.{SchemaId, SchemaVersion}
import hydra.avro.util.SchemaWrapper
import hydra.common.NotificationsTestSuite
import hydra.common.alerting.sender.InternalNotificationSender
import hydra.common.config.KafkaConfigUtils.{KafkaClientSecurityConfig, kafkaSecurityEmptyConfig}
import hydra.core.http.security.{AccessControlService, AwsSecurityService}
import hydra.core.http.security.entity.AwsConfig
import hydra.ingest.programs.TopicDeletionProgram
import hydra.kafka.algebras.KafkaAdminAlgebra._
import hydra.kafka.algebras.RetryableFs2Stream.RetryPolicy.Once
import hydra.kafka.algebras.{ConsumerGroupsAlgebra, KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.kafka.clients.admin.ConsumerGroupDescription
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import retry.Sleep
import scalacache.Cache
import scalacache.guava.GuavaCache

import scala.concurrent.ExecutionContext



class TopicDeletionEndpointSpec extends Matchers with AnyWordSpecLike with ScalatestRouteTest with NotificationsTestSuite{
  implicit private val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect
  private val v2MetadataTopicName = Subject.createValidated("_test.V2.MetadataTopic").get
  private val v1MetadataTopicName = Subject.createValidated("_test.V1.MetadataTopic").get
  private val consumerGroup = "consumer groups"
  implicit val guavaCache: Cache[SchemaWrapper] = GuavaCache[SchemaWrapper]
  implicit val timer: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)
  private val awsSecurityService = mock[AwsSecurityService[IO]]
  private val noAuth = new AccessControlService[IO](awsSecurityService, AwsConfig("somecluster", isAwsIamSecurityEnabled = false))

  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] =
    Slf4jLogger.getLogger[F]

  def schemaBadTest[F[_]: Sync: Sleep](simulateV1BadDeletion: Boolean, simulateV2BadDeletion: Boolean): F[SchemaRegistry[F]] =
    SchemaRegistry.test[F].map(sr => getFromBadSchemaRegistryClient[F](sr, simulateV1BadDeletion, simulateV2BadDeletion))

  private def getFromBadSchemaRegistryClient[F[_]: Sync](underlying: SchemaRegistry[F], simulateV1BadDeletion: Boolean, simulateV2BadDeletion: Boolean): SchemaRegistry[F] =
    new SchemaRegistry[F] {

      override def registerSchema(subject: String,schema: Schema): F[SchemaId] = {
        underlying.registerSchema(subject, schema)
      }

      override def deleteSchemaOfVersion(subject: String,version: SchemaVersion): F[Unit] =
        underlying.deleteSchemaOfVersion(subject,version)

      override def getVersion(subject: String,schema: Schema): F[SchemaVersion] =
        underlying.getVersion(subject,schema)

      override def getAllVersions(subject: String): F[List[SchemaId]] =
        underlying.getAllVersions(subject)

      override def getAllSubjects: F[List[String]] =
        underlying.getAllSubjects

      override def getSchemaRegistryClient: F[SchemaRegistryClient] = underlying.getSchemaRegistryClient

      override def getLatestSchemaBySubject(subject: String): F[Option[Schema]] = underlying.getLatestSchemaBySubject(subject)

      override def getSchemaFor(subject: String, schemaVersion: SchemaVersion): F[Option[Schema]] = underlying.getSchemaFor(subject, schemaVersion)

      override def deleteSchemaSubject(subject: String): F[Unit] =
        if(simulateV1BadDeletion && subject.contains("-value")) {
          Sync[F].raiseError(new Exception("Unable to delete schema"))
        }
        else if(simulateV2BadDeletion) {
          Sync[F].raiseError(new Exception("Unable to delete schema"))
        }
        else {
          underlying.deleteSchemaSubject(subject)
        }
    }

  def kafkaBadTest[F[_] : Sync]: F[KafkaAdminAlgebra[F]] =
    KafkaAdminAlgebra.test[F]().flatMap( kaa => getBadTestKafkaClient[F](kaa))

  private[this] def getBadTestKafkaClient[F[_] : Sync](underlying: KafkaAdminAlgebra[F]): F[KafkaAdminAlgebra[F]] = Sync[F].delay {
    new KafkaAdminAlgebra[F] {
      override def describeTopic(name: TopicName): F[Option[Topic]] = underlying.describeTopic(name)

      override def getTopicNames: F[List[TopicName]] =
        underlying.getTopicNames

      override def createTopic(name: TopicName, details: TopicDetails): F[Unit] = underlying.createTopic(name, details)

      override def deleteTopic(name: String): F[Unit] = ???

      // This is intentionally unimplemented. This test class has no way of obtaining this offset information.
      override def getConsumerGroupOffsets(consumerGroup: String): F[Map[TopicAndPartition, Offset]] = ???

      // This is intentionally unimplemented. This test class has no way of obtaining this offset information.
      override def getLatestOffsets(topic: TopicName): F[Map[TopicAndPartition, Offset]] = {
        Sync[F].pure(Map.empty[TopicAndPartition, Offset])
      }

      // This is intentionally unimplemented. This test class has no way of obtaining this offset information.
      override def getConsumerLag(topic: TopicName, consumerGroup: String): F[Map[TopicAndPartition, LagOffsets]] = ???

      override def deleteTopics(topicNames: List[String]): F[Either[KafkaDeleteTopicErrorList, Unit]] = {
          Sync[F].pure(Left(new KafkaDeleteTopicErrorList(NonEmptyList.fromList(
            topicNames.map(topic => KafkaDeleteTopicError(topic, new Exception("Unable to delete topic")))).get)))
      }

      override def describeConsumerGroup(consumerGroupName: String): F[Option[ConsumerGroupDescription]] = ???
    }
  }

  private def buildSchema(topic: String, upgrade: Boolean): Schema = {
    val schemaStart = SchemaBuilder.record("name" + topic.replace("-", "").replace(".", ""))
      .fields().requiredString("id" + topic.replace("-", "").replace(".", ""))
    if (upgrade && !topic.contains("-key")) {
      schemaStart.nullableBoolean("upgrade", upgrade).endRecord()
    } else {
      schemaStart.endRecord()
    }
  }

  private def registerTopics(topicNames: List[String], schemaAlgebra: SchemaRegistry[IO],
                             registerKey: Boolean, upgrade: Boolean): IO[List[SchemaId]] = {
    topicNames.flatMap(topic => if (registerKey) List(topic + "-key", topic + "-value") else List(topic + "-value"))
      .traverse(topic => schemaAlgebra.registerSchema(topic, buildSchema(topic, upgrade)))
  }


  def getConsumerAlgebra(kafkaClientAlgebra: KafkaClientAlgebra[IO],
                         kafkaAdminAlgebra: KafkaAdminAlgebra[IO],
                         schemaAlgebra: SchemaRegistry[IO]): IO[ConsumerGroupsAlgebra[IO]] = {
    implicit val notificationSenderMock: InternalNotificationSender[IO] = getInternalNotificationSenderMock[IO]
    ConsumerGroupsAlgebra.make("",Subject.createValidated("dvs.blah.blah").get,
      Subject.createValidated("dvs.heyo.blah").get,"","","",
      kafkaClientAlgebra,kafkaAdminAlgebra,schemaAlgebra, kafkaSecurityEmptyConfig)
  }

  "The deletionEndpoint path" should {

    val validCredentials = BasicHttpCredentials("John", "myPass")

    "return 200 with single deletion in body" in {
      val topic = List("exp.blah.blah", v2MetadataTopicName.toString, v1MetadataTopicName.toString)
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]()
        schemaAlgebra <- SchemaRegistry.test[IO]
        kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
        metadataAlgebra <- makeMetadataAlgebra(kafkaClientAlgebra, schemaAlgebra, false)
        consumerGroupAlgebra <- getConsumerAlgebra(kafkaClientAlgebra,kafkaAlgebra,schemaAlgebra)
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t.toString,TopicDetails(1,1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        allTopics <- kafkaAlgebra.getTopicNames
      } yield {
        allTopics shouldBe topic
        val route = new TopicDeletionEndpoint[IO](
          new TopicDeletionProgram[IO](
            kafkaAlgebra,
            kafkaClientAlgebra,
            v2MetadataTopicName,
            v1MetadataTopicName,
            schemaAlgebra,
            metadataAlgebra,
            consumerGroupAlgebra,
            List.empty,
            0
          ),
          "myPass", noAuth, awsSecurityService).route
        Delete("/v2/topics", HttpEntity(ContentTypes.`application/json`, """{"topics":["exp.blah.blah"]}""")) ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe """["exp.blah.blah"]"""
          status shouldBe StatusCodes.OK
        }
      }).unsafeRunSync()
    }

    "return 200 with single schema deletion" in {
      val topic = List("exp.blah.blah", v2MetadataTopicName.toString, v1MetadataTopicName.toString)
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]()
        schemaAlgebra <- SchemaRegistry.test[IO]
        kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
        metadataAlgebra <- makeMetadataAlgebra(kafkaClientAlgebra, schemaAlgebra)
        consumerGroupAlgebra <- getConsumerAlgebra(kafkaClientAlgebra,kafkaAlgebra,schemaAlgebra)
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        allTopics <- kafkaAlgebra.getTopicNames
      } yield {
        allTopics shouldBe topic
        val route = new TopicDeletionEndpoint[IO](
          new TopicDeletionProgram[IO](
            kafkaAlgebra,
            kafkaClientAlgebra,
            v2MetadataTopicName,
            v1MetadataTopicName,
            schemaAlgebra,
            metadataAlgebra,
            consumerGroupAlgebra,
            List.empty,
            0
          ), "myPass", noAuth, awsSecurityService).route
        Delete("/v2/topics/schemas/exp.blah.blah") ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe """["exp.blah.blah"]"""
          status shouldBe StatusCodes.OK
        }
      }).unsafeRunSync()
    }

    "return 200 with multiple deletions" in {
      val topic = List("exp.blah.blah","exp.hello.world","exp.hi.there")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]()
        schemaAlgebra <- SchemaRegistry.test[IO]
        kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
        metadataAlgebra <- makeMetadataAlgebra(kafkaClientAlgebra, schemaAlgebra)
        consumerGroupAlgebra <- getConsumerAlgebra(kafkaClientAlgebra,kafkaAlgebra,schemaAlgebra)
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
      } yield {
        val route = new TopicDeletionEndpoint[IO](
          new TopicDeletionProgram[IO](
            kafkaAlgebra,
            kafkaClientAlgebra,
            v2MetadataTopicName,
            v1MetadataTopicName,
            schemaAlgebra,
            metadataAlgebra,
            consumerGroupAlgebra,
            List.empty,
            0
          ), "myPass", noAuth, awsSecurityService).route
        Delete("/v2/topics", HttpEntity(ContentTypes.`application/json`, """{"topics":["exp.blah.blah","exp.hello.world","exp.hi.there"]}""")) ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe "[\"exp.blah.blah\",\"exp.hello.world\",\"exp.hi.there\"]"
          status shouldBe StatusCodes.OK
        }
      }).unsafeRunSync()
    }

    "return 500 if one topic not successful from Kafka" in {
      val topic = List("exp.blah.blah")
      (for {
        kafkaAlgebra <- kafkaBadTest[IO]
        schemaAlgebra <- SchemaRegistry.test[IO]
        kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
        metadataAlgebra <- makeMetadataAlgebra(kafkaClientAlgebra, schemaAlgebra)
        consumerGroupAlgebra <- getConsumerAlgebra(kafkaClientAlgebra,kafkaAlgebra,schemaAlgebra)
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
      } yield {
        val route = new TopicDeletionEndpoint[IO](
          new TopicDeletionProgram[IO](
            kafkaAlgebra,
            kafkaClientAlgebra,
            v2MetadataTopicName,
            v1MetadataTopicName,
            schemaAlgebra,
            metadataAlgebra,
            consumerGroupAlgebra,
            List.empty,
            0
          ), "myPass", noAuth, awsSecurityService).route
        Delete("/v2/topics", HttpEntity(ContentTypes.`application/json`, """{"topics":["exp.blah.blah"]}""")) ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe """{"clientError":[],"serverError":[{"message":"exp.blah.blah Unable to delete topic","responseCode":500,"topicOrSubject":"exp.blah.blah"}],"success":[]}"""
          status shouldBe StatusCodes.InternalServerError
        }
      }).unsafeRunSync()
    }

    "return 200 with single deletion in url" in {
      val topic = List("exp.blah.blah")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]()
        schemaAlgebra <- SchemaRegistry.test[IO]
        kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
        metadataAlgebra <- makeMetadataAlgebra(kafkaClientAlgebra, schemaAlgebra)
        consumerGroupAlgebra <- getConsumerAlgebra(kafkaClientAlgebra,kafkaAlgebra,schemaAlgebra)
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        allTopics <- kafkaAlgebra.getTopicNames
      } yield {
        allTopics shouldBe topic
        val route = new TopicDeletionEndpoint[IO](
          new TopicDeletionProgram[IO](
            kafkaAlgebra,
            kafkaClientAlgebra,
            v2MetadataTopicName,
            v1MetadataTopicName,
            schemaAlgebra,
            metadataAlgebra,
            consumerGroupAlgebra,
            List.empty,
            0
          ), "myPass", noAuth, awsSecurityService).route
        Delete("/v2/topics/exp.blah.blah") ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe """["exp.blah.blah"]"""
          status shouldBe StatusCodes.OK
        }
      }).unsafeRunSync()
    }

    "return 401 with bad credentials" in {
      val topic = List("exp.blah.blah")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]()
        schemaAlgebra <- SchemaRegistry.test[IO]
        kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
        metadataAlgebra <- makeMetadataAlgebra(kafkaClientAlgebra, schemaAlgebra)
        consumerGroupAlgebra <- getConsumerAlgebra(kafkaClientAlgebra,kafkaAlgebra,schemaAlgebra)
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        allTopics <- kafkaAlgebra.getTopicNames
      } yield {
        allTopics shouldBe topic
        val route = new TopicDeletionEndpoint[IO](
          new TopicDeletionProgram[IO](
            kafkaAlgebra,
            kafkaClientAlgebra,
            v2MetadataTopicName,
            v1MetadataTopicName,
            schemaAlgebra,
            metadataAlgebra,
            consumerGroupAlgebra,
            List.empty,
            0
          ), "myPass", noAuth, awsSecurityService).route
        Delete("/v2/topics", HttpEntity(ContentTypes.`application/json`, """{"topics":["exp.blah.blah"]}""")) ~>
          addCredentials(BasicHttpCredentials("John", "badPass")) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe "The supplied authentication is invalid"
          status shouldBe StatusCodes.Unauthorized
        }
      }).unsafeRunSync()
    }

    "return 401 with no credentials" in {
      val topic = List("exp.blah.blah")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]()
        schemaAlgebra <- SchemaRegistry.test[IO]
        kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
        metadataAlgebra <- makeMetadataAlgebra(kafkaClientAlgebra, schemaAlgebra)
        consumerGroupAlgebra <- getConsumerAlgebra(kafkaClientAlgebra,kafkaAlgebra,schemaAlgebra)
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        allTopics <- kafkaAlgebra.getTopicNames
      } yield {
        allTopics shouldBe topic
        val route = new TopicDeletionEndpoint[IO](
          new TopicDeletionProgram[IO](
            kafkaAlgebra,
            kafkaClientAlgebra,
            v2MetadataTopicName,
            v1MetadataTopicName,
            schemaAlgebra,
            metadataAlgebra,
            consumerGroupAlgebra,
            List.empty,
            0
          ), "myPass", noAuth, awsSecurityService).route
        Delete("/v2/topics", HttpEntity(ContentTypes.`application/json`, """{"topics":["exp.blah.blah"]}""")) ~>
          Route.seal(route) ~> check {
          responseAs[String] shouldBe "The resource requires authentication, which was not supplied with the request"
          status shouldBe StatusCodes.Unauthorized
        }
      }).unsafeRunSync()
    }

    "return 207 with a schema failure -value only" in {
      val topic = List("exp.blah.blah")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]()
        schemaAlgebra <- schemaBadTest[IO](true, false)
        kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
        metadataAlgebra <- makeMetadataAlgebra(kafkaClientAlgebra, schemaAlgebra)
        consumerGroupAlgebra <- getConsumerAlgebra(kafkaClientAlgebra,kafkaAlgebra,schemaAlgebra)
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        allTopics <- kafkaAlgebra.getTopicNames
      } yield {
        allTopics shouldBe topic
        val route = new TopicDeletionEndpoint[IO](
          new TopicDeletionProgram[IO](
            kafkaAlgebra,
            kafkaClientAlgebra,
            v2MetadataTopicName,
            v1MetadataTopicName,
            schemaAlgebra,
            metadataAlgebra,
            consumerGroupAlgebra,
            List.empty,
            0
          ), "myPass", noAuth, awsSecurityService).route
        Delete("/v2/topics/exp.blah.blah") ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe """{"clientError":[],"serverError":[{"message":"Unable to delete schemas for exp.blah.blah-value. If this is a V1 topic, a key schema deletion error is normal. java.lang.Exception: Unable to delete schema","responseCode":500,"topicOrSubject":"exp.blah.blah-value"}],"success":[{"responseCode":200,"topicOrSubject":"exp.blah.blah"}]}"""
          status shouldBe StatusCodes.MultiStatus
        }
      }).unsafeRunSync()
    }

    "return 207 with a schema failure V2" in {
      val topic = List("exp.blah.blah")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]()
        schemaAlgebra <- schemaBadTest[IO](false, true)
        kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
        metadataAlgebra <- makeMetadataAlgebra(kafkaClientAlgebra, schemaAlgebra)
        consumerGroupAlgebra <- getConsumerAlgebra(kafkaClientAlgebra,kafkaAlgebra,schemaAlgebra)
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        allTopics <- kafkaAlgebra.getTopicNames
      } yield {
        allTopics shouldBe topic
        val route = new TopicDeletionEndpoint[IO](
          new TopicDeletionProgram[IO](
            kafkaAlgebra,
            kafkaClientAlgebra,
            v2MetadataTopicName,
            v1MetadataTopicName,
            schemaAlgebra,
            metadataAlgebra,
            consumerGroupAlgebra,
            List.empty,
            0
          ), "myPass", noAuth, awsSecurityService).route
        Delete("/v2/topics/exp.blah.blah") ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe """{"clientError":[],"serverError":[{"message":"Unable to delete schemas for exp.blah.blah-key. If this is a V1 topic, a key schema deletion error is normal. java.lang.Exception: Unable to delete schema","responseCode":500,"topicOrSubject":"exp.blah.blah-key"},{"message":"Unable to delete schemas for exp.blah.blah-value. If this is a V1 topic, a key schema deletion error is normal. java.lang.Exception: Unable to delete schema","responseCode":500,"topicOrSubject":"exp.blah.blah-value"}],"success":[{"responseCode":200,"topicOrSubject":"exp.blah.blah"}]}"""
          status shouldBe StatusCodes.MultiStatus
        }
      }).unsafeRunSync()
    }

    "return 207 with a schema failure schema endpoint -value only" in {
      val topic = List("exp.blah.blah")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]()
        schemaAlgebra <- schemaBadTest[IO](true, false)
        kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
        metadataAlgebra <- makeMetadataAlgebra(kafkaClientAlgebra, schemaAlgebra)
        consumerGroupAlgebra <- getConsumerAlgebra(kafkaClientAlgebra,kafkaAlgebra,schemaAlgebra)
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        allTopics <- kafkaAlgebra.getTopicNames
      } yield {
        allTopics shouldBe topic
        val route = new TopicDeletionEndpoint[IO](
          new TopicDeletionProgram[IO](
            kafkaAlgebra,
            kafkaClientAlgebra,
           v2MetadataTopicName,
            v1MetadataTopicName,
            schemaAlgebra,
            metadataAlgebra,
            consumerGroupAlgebra,
            List.empty,
            0
          ), "myPass", noAuth, awsSecurityService).route
        Delete("/v2/topics/schemas/exp.blah.blah") ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe """{"clientError":[{"message":"Unable to delete schemas for exp.blah.blah-value. If this is a V1 topic, a key schema deletion error is normal. java.lang.Exception: Unable to delete schema","responseCode":500,"topicOrSubject":"exp.blah.blah-value"}],"serverError":[],"success":[]}"""
          status shouldBe StatusCodes.MultiStatus
        }
      }).unsafeRunSync()
    }

    "return 500 with a schema failure schema endpoint V2" in {
      val topic = List("exp.blah.blah")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]()
        schemaAlgebra <- schemaBadTest[IO](false, true)
        kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
        metadataAlgebra <- makeMetadataAlgebra(kafkaClientAlgebra, schemaAlgebra)
        consumerGroupAlgebra <- getConsumerAlgebra(kafkaClientAlgebra,kafkaAlgebra,schemaAlgebra)
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        allTopics <- kafkaAlgebra.getTopicNames
      } yield {
        allTopics shouldBe topic
        val route = new TopicDeletionEndpoint[IO](
          new TopicDeletionProgram[IO](
            kafkaAlgebra,
            kafkaClientAlgebra,
            v2MetadataTopicName,
            v1MetadataTopicName,
            schemaAlgebra,
            metadataAlgebra,
            consumerGroupAlgebra,
            List.empty,
            0
          ), "myPass", noAuth, awsSecurityService).route
        Delete("/v2/topics/schemas/exp.blah.blah") ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe """{"clientError":[{"message":"Unable to delete schemas for exp.blah.blah-key. If this is a V1 topic, a key schema deletion error is normal. java.lang.Exception: Unable to delete schema","responseCode":500,"topicOrSubject":"exp.blah.blah-key"},{"message":"Unable to delete schemas for exp.blah.blah-value. If this is a V1 topic, a key schema deletion error is normal. java.lang.Exception: Unable to delete schema","responseCode":500,"topicOrSubject":"exp.blah.blah-value"}],"serverError":[],"success":[]}"""
          status shouldBe StatusCodes.InternalServerError
        }
      }).unsafeRunSync()
    }

    "return 400 when requested to delete topic that does not exist endpoint V2" in {
      val topic = List("exp.blah.blah")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]()
        schemaAlgebra <- SchemaRegistry.test[IO]
        kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
        metadataAlgebra <- makeMetadataAlgebra(kafkaClientAlgebra, schemaAlgebra)
        consumerGroupAlgebra <- getConsumerAlgebra(kafkaClientAlgebra,kafkaAlgebra,schemaAlgebra)
      } yield {
        val route = new TopicDeletionEndpoint[IO](
          new TopicDeletionProgram[IO](
            kafkaAlgebra,
            kafkaClientAlgebra,
            v2MetadataTopicName,
            v1MetadataTopicName,
            schemaAlgebra,
            metadataAlgebra,
            consumerGroupAlgebra,
            List.empty,
            0
          ), "myPass", noAuth, awsSecurityService).route
        Delete("/v2/topics/exp.blah.blah") ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe """{"clientError":[{"message":"The requested topic does not exist.","responseCode":404,"topicOrSubject":"exp.blah.blah"}],"serverError":[],"success":[]}"""
          status shouldBe StatusCodes.BadRequest
        }
      }).unsafeRunSync()
    }
  }

  def makeMetadataAlgebra(kafkaClientAlgebra: KafkaClientAlgebra[IO], schemaRegistry: SchemaRegistry[IO], consumerMetadataEnabled: Boolean = true): IO[MetadataAlgebra[IO]] = {
    implicit val notificationSenderMock: InternalNotificationSender[IO] = getInternalNotificationSenderMock[IO]
    MetadataAlgebra.make(v2MetadataTopicName, consumerGroup, kafkaClientAlgebra, schemaRegistry, consumerMetadataEnabled, Once)
  }

}
