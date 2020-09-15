package hydra.ingest.http

import cats.data.NonEmptyList
import cats.effect.{IO, Sync}
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.KafkaAdminAlgebra
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.matchers.should.Matchers
import cats.implicits._
import hydra.avro.registry.SchemaRegistry.{SchemaId, SchemaVersion}
import hydra.ingest.programs.TopicDeletionProgram
import hydra.kafka.algebras.KafkaAdminAlgebra.{KafkaDeleteTopicError, KafkaDeleteTopicErrorList, LagOffsets, Offset, Topic, TopicAndPartition, TopicName}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.scalatest.wordspec.{AnyWordSpec, AnyWordSpecLike}
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit._



class TopicDeletionEndpointSpec extends Matchers with AnyWordSpecLike with ScalatestRouteTest{

  import concurrent.ExecutionContext.Implicits.global

  def schemaBadTest[F[_] : Sync](allowAllVersions: Boolean = true, errorOnUpgrade: Boolean = false): F[SchemaRegistry[F]] =
    SchemaRegistry.test[F].map(sr => getFromBadSchemaRegistryClient[F](sr, allowAllVersions, errorOnUpgrade))

  private def getFromBadSchemaRegistryClient[F[_] : Sync](underlying: SchemaRegistry[F], allowAllVersions: Boolean, errorOnUpgrade: Boolean): SchemaRegistry[F] =
    new SchemaRegistry[F] {

      override def registerSchema(subject: String, schema: Schema): F[SchemaId] = {
        underlying.registerSchema(subject, schema)
      }

      override def deleteSchemaOfVersion(subject: String, version: SchemaVersion): F[Unit] =
        if (errorOnUpgrade && version > 1) {
          Sync[F].raiseError(new Exception(s"Error on version $version"))
        } else {
          underlying.deleteSchemaOfVersion(subject, version)
        }

      override def getVersion(subject: String, schema: Schema): F[SchemaVersion] =
        underlying.getVersion(subject, schema)

      override def getAllVersions(subject: String): F[List[SchemaId]] =
        if (allowAllVersions) underlying.getAllVersions(subject)
        else if(!allowAllVersions && subject.contains("-key")) underlying.getAllVersions(subject)
        else Sync[F].raiseError(new Exception("Unable to get all versions"))

      override def getAllSubjects: F[List[String]] =
        underlying.getAllSubjects

      override def getSchemaRegistryClient: F[SchemaRegistryClient] = underlying.getSchemaRegistryClient

      override def getLatestSchemaBySubject(subject: String): F[Option[Schema]] = underlying.getLatestSchemaBySubject(subject)

      override def getSchemaFor(subject: String, schemaVersion: SchemaVersion): F[Option[Schema]] = underlying.getSchemaFor(subject, schemaVersion)

    }

  def kafkaBadTest[F[_] : Sync]: F[KafkaAdminAlgebra[F]] =
    KafkaAdminAlgebra.test[F].flatMap(getBadTestKafkaClient[F])

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
      override def getLatestOffsets(topic: TopicName): F[Map[TopicAndPartition, Offset]] = ???

      // This is intentionally unimplemented. This test class has no way of obtaining this offset information.
      override def getConsumerLag(topic: TopicName, consumerGroup: String): F[Map[TopicAndPartition, LagOffsets]] = ???

      override def kafkaContainsTopic(name: TopicName): F[Boolean] = getTopicNames.map(topics => false)

      override def deleteTopics(topicNames: List[String]): F[Either[KafkaDeleteTopicErrorList, Unit]] =
        Sync[F].pure(Left(new KafkaDeleteTopicErrorList(NonEmptyList.fromList(
          topicNames.map(topic => KafkaDeleteTopicError(topic, new Exception("Unable to delete topic")))).get)))
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


  "The deletionEndpoint path" should {

    val validCredentials = BasicHttpCredentials("John", "myPass")

    "return 200 with single deletion in body" in {
      val topic = List("exp.blah.blah")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]
        schemaAlgebra <- SchemaRegistry.test[IO]
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        allTopics <- kafkaAlgebra.getTopicNames
      } yield {
        allTopics shouldBe topic
        val route = new TopicDeletionEndpoint[IO](new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra), "myPass").route
        Delete("/v2/topics", HttpEntity(ContentTypes.`application/json`, """{"topics":["exp.blah.blah"]}""")) ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe """["exp.blah.blah"]"""
          status shouldBe StatusCodes.OK
        }
      }).unsafeRunSync()
    }

    "return 200 with single schema deletion" in {
      val topic = List("exp.blah.blah")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]
        schemaAlgebra <- SchemaRegistry.test[IO]
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        allTopics <- kafkaAlgebra.getTopicNames
      } yield {
        allTopics shouldBe topic
        val route = new TopicDeletionEndpoint[IO](new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra), "myPass").route
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
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]
        schemaAlgebra <- SchemaRegistry.test[IO]
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
      } yield {
        val route = new TopicDeletionEndpoint[IO](new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra), "myPass").route
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
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
      } yield {
        val route = new TopicDeletionEndpoint[IO](new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra), "myPass").route
        Delete("/v2/topics", HttpEntity(ContentTypes.`application/json`, """{"topics":["exp.blah.blah"]}""")) ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe "[{\"message\":\"exp.blah.blah Unable to delete topic\",\"topicOrSubject\":\"exp.blah.blah\"}]"
          status shouldBe StatusCodes.InternalServerError
        }
      }).unsafeRunSync()
    }

    "return 500 if unable to get all schema versions for -value" in {
      val topic = List("exp.blah.blah")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]
        schemaAlgebra <- schemaBadTest[IO](allowAllVersions = false)
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
      } yield {
        val route = new TopicDeletionEndpoint[IO](new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra), "myPass").route
        Delete("/v2/topics", HttpEntity(ContentTypes.`application/json`, """{"topics":["exp.blah.blah"]}""")) ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe
            """[{"message":"Unable to get all schema versions for exp.blah.blah-value java.lang.Exception: Unable to get all versions","topicOrSubject":"exp.blah.blah-value"}]""".stripMargin
          status shouldBe StatusCodes.InternalServerError
        }
      }).unsafeRunSync()
    }

    "return 202 if unable to delete specific version" in {
      val topic = List("exp.blah.blah")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]
        schemaAlgebra <- schemaBadTest[IO](errorOnUpgrade = true)
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        _ <- registerTopics(topic, schemaAlgebra, registerKey= false, upgrade = true)
      } yield {
        val route = new TopicDeletionEndpoint[IO](new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra), "myPass").route
        Delete("/v2/topics", HttpEntity(ContentTypes.`application/json`, """{"topics":["exp.blah.blah"]}""")) ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe """[{"message":"Failed to delete version: 2 for exp.blah.blah-value java.lang.Exception: Error on version 2","topicOrSubject":"exp.blah.blah-value"}]"""
          status shouldBe StatusCodes.InternalServerError
        }
      }).unsafeRunSync()
    }

    "return 202 if unable to delete specific version -value, but all others pass" in {
      val topics = List("exp.blah.blah","exp.blah.ha","exp.test.this","exp.hi.There")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]
        schemaAlgebra <- schemaBadTest[IO](errorOnUpgrade = true)
        _ <- topics.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1)))
        _ <- registerTopics(topics, schemaAlgebra, registerKey = false, upgrade = false)
        _ <- registerTopics(List("exp.blah.ha"), schemaAlgebra, registerKey= false, upgrade = true)
      } yield {
        val route = new TopicDeletionEndpoint[IO](new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra), "myPass").route
        Delete("/v2/topics", HttpEntity(ContentTypes.`application/json`, """{"topics":["exp.blah.blah", "exp.blah.ha", "exp.test.this", "exp.hi.There"]}""")) ~>
          addCredentials(validCredentials) ~> Route.seal(route) ~> check {
          responseAs[String] shouldBe """[{"message":"Failed to delete version: 2 for exp.blah.ha-value java.lang.Exception: Error on version 2","topicOrSubject":"exp.blah.ha-value"}]"""
          status shouldBe StatusCodes.Accepted
        }
      }).unsafeRunSync()
    }

    "return 200 with single deletion in url" in {
      val topic = List("exp.blah.blah")
      (for {
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]
        schemaAlgebra <- SchemaRegistry.test[IO]
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        allTopics <- kafkaAlgebra.getTopicNames
      } yield {
        allTopics shouldBe topic
        val route = new TopicDeletionEndpoint[IO](new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra), "myPass").route
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
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]
        schemaAlgebra <- SchemaRegistry.test[IO]
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        allTopics <- kafkaAlgebra.getTopicNames
      } yield {
        allTopics shouldBe topic
        val route = new TopicDeletionEndpoint[IO](new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra), "myPass").route
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
        kafkaAlgebra <- KafkaAdminAlgebra.test[IO]
        schemaAlgebra <- SchemaRegistry.test[IO]
        _ <- topic.traverse(t => kafkaAlgebra.createTopic(t,TopicDetails(1,1)))
        _ <- registerTopics(topic, schemaAlgebra, registerKey = false, upgrade = false)
        allTopics <- kafkaAlgebra.getTopicNames
      } yield {
        allTopics shouldBe topic
        val route = new TopicDeletionEndpoint[IO](new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra), "myPass").route
        Delete("/v2/topics", HttpEntity(ContentTypes.`application/json`, """{"topics":["exp.blah.blah"]}""")) ~>
          Route.seal(route) ~> check {
          responseAs[String] shouldBe "The resource requires authentication, which was not supplied with the request"
          status shouldBe StatusCodes.Unauthorized
        }
      }).unsafeRunSync()
    }

  }

}
