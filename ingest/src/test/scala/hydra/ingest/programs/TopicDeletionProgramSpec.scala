package hydra.ingest.programs

import java.time.Instant

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.{Bracket, Concurrent, ContextShift, IO, Sync, Timer}
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import cats.implicits._
import hydra.avro.registry.SchemaRegistry.{SchemaId, SchemaVersion}
import hydra.kafka.algebras.KafkaAdminAlgebra.{KafkaDeleteTopicError, KafkaDeleteTopicErrorList, LagOffsets, Offset, Topic, TopicAndPartition, TopicName}
import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataContainer
import hydra.kafka.model.ContactMethod.Email
import hydra.kafka.model.{Public, Schemas, StreamTypeV2, TopicMetadataV2, TopicMetadataV2Key, TopicMetadataV2Request}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.programs.CreateTopicProgram
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import retry.{RetryPolicies, RetryPolicy}

import scala.concurrent.ExecutionContext

class TopicDeletionProgramSpec extends AnyFlatSpec with Matchers {
  implicit private val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect
  private val v2MetadataTopicName = "_test.V2.MetadataTopic"
  private val v1MetadataTopicName = "_test.V1.MetadataTopic"
  private val consumerGroup = "consumer groups"
  implicit val timer: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)


  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] =
    Slf4jLogger.getLogger[F]

  def schemaBadTest[F[_]: Sync](simulateBadDeletion: Boolean): F[SchemaRegistry[F]] =
    SchemaRegistry.test[F].map(sr => getFromBadSchemaRegistryClient[F](sr, simulateBadDeletion))

  private def getFromBadSchemaRegistryClient[F[_]: Sync](underlying: SchemaRegistry[F], simulateBadDeletion: Boolean): SchemaRegistry[F] =
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

      //TODO: Test this
      override def getLatestSchemaBySubject(subject: String): F[Option[Schema]] = underlying.getLatestSchemaBySubject(subject)

      override def getSchemaFor(subject: String, schemaVersion: SchemaVersion): F[Option[Schema]] = underlying.getSchemaFor(subject, schemaVersion)

      override def deleteSchemaSubject(subject: String): F[Unit] =
        if(simulateBadDeletion) {
          Sync[F].raiseError(new Exception("Unable to delete schema"))
        }
        else {
          underlying.deleteSchemaSubject(subject)
        }
    }

  def kafkabadTest[F[_]: Sync]: F[KafkaAdminAlgebra[F]] =
    KafkaAdminAlgebra.test[F].flatMap(getBadTestKafkaClient[F])

  private[this] def getBadTestKafkaClient[F[_]: Sync](underlying: KafkaAdminAlgebra[F]): F[KafkaAdminAlgebra[F]] = Sync[F].delay  {
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

      override def deleteTopics(topicNames: List[String]): F[Either[KafkaDeleteTopicErrorList, Unit]] =
        Sync[F].pure(Left(new KafkaDeleteTopicErrorList( NonEmptyList.fromList(
          topicNames.map(topic => KafkaDeleteTopicError(topic, new Exception("Unable to delete topic")))).get)))
    }
  }

  private val twoTopics = List("topic1", "topic2")
  private val invalidErrorChecker: ErrorChecker = errors => errors shouldBe a [Invalid[_]]
  private val noUpgrade = ("",false)
  private type ErrorChecker = ValidatedNel[DeleteTopicError, Unit] => Unit

  private def createTopicMetadataRequest(
                                          keySchema: Schema,
                                          valueSchema: Schema,
                                          email: String = "test@test.com",
                                          createdDate: Instant = Instant.now(),
                                          deprecated: Boolean = false,
                                          deprecatedDate: Option[Instant] = None
                                        ): TopicMetadataV2Request =
    TopicMetadataV2Request(
      Schemas(keySchema, valueSchema),
      StreamTypeV2.Entity,
      deprecated = deprecated,
      deprecatedDate,
      Public,
      NonEmptyList.of(Email.create(email).get),
      createdDate,
      List.empty,
      None,
      Some("dvs-teamName")
    )

  private def buildSchema(topic: String, upgrade: Boolean): Schema = {
    val schemaStart = SchemaBuilder.record("name" + topic.replace("-", "").replace(".", ""))
      .fields().requiredString("id" + topic.replace("-", "").replace(".", ""))
    if (upgrade && !topic.contains("-key")) {
      schemaStart.nullableBoolean("upgrade",upgrade).endRecord()
    } else {
      schemaStart.endRecord()
    }
  }

  private def registerTopics(topicNames: List[String], schemaAlgebra: SchemaRegistry[IO],
                                    registerKey: Boolean, upgrade: Boolean): IO[List[SchemaId]] = {
    topicNames.flatMap(topic => if(registerKey) List(topic + "-key", topic + "-value") else List(topic + "-value"))
      .traverse(topic => schemaAlgebra.registerSchema(topic, buildSchema(topic, upgrade)))
  }

  private def possibleFailureGetAllSchemaVersions(topicNames: List[String], schemaAlgebra: SchemaRegistry[IO]): IO[List[String]] = {
    topicNames.traverse(topic => schemaAlgebra.getAllVersions(topic + "-value").attempt.map {
      case Right(versions) => if(versions.nonEmpty) Some(topic + "-value") else None
      case Left(_) => Some(topic + "-value")
    }).map(_.flatten)
  }

  private def writeV2TopicMetadata(topics: List[String], metadataAlgebra: MetadataAlgebra[IO]) = {
    topics.traverse(topic => {
      val keySchema = buildSchema(topic + "-key", false)
      val valueSchema = buildSchema(topic + "-value", false)
      val topicMetadataKey = TopicMetadataV2Key(Subject.createValidated(topic).get)
      val req = createTopicMetadataRequest(keySchema, valueSchema)
      val topicMetadataContainer = TopicMetadataContainer(topicMetadataKey, req.toValue, keySchema.some, valueSchema.some)
      metadataAlgebra.addMetadata(topicMetadataContainer)
    })
  }

  private def getExpectedDeletedTopics(topicNames: List[String], topicNamesToDelete: List[String], kafkaTopicNamesToFail: List[String]): List[String] = {
    return topicNames.toSet.intersect(topicNamesToDelete.toSet).diff(kafkaTopicNamesToFail.toSet).toList
  }

  private def applyTestcase(kafkaAdminAlgebra: IO[KafkaAdminAlgebra[IO]],
                            schemaRegistry: IO[SchemaRegistry[IO]],
                            v1TopicNames: List[String],
                            v2TopicNames: List[String],
                            topicNamesToDelete: List[String],
                            schemasToSucceed: List[String] = List.empty,
                            registerKey: Boolean,
                            kafkaTopicNamesToFail: List[String] = List.empty,
                            upgradeTopic: Tuple2[String, Boolean] = ("", false),
                            assertionError: ErrorChecker = _ => ()): Unit = {
    (for {
      // For v2 topics we need to write the metadata to the v2MetadataTopic because the topic deletion attempts to lookup
      // the v2 metadata and uses the results to determine if we are deleting a v1 or v2 topic.

      kafkaAdmin <- kafkaAdminAlgebra
      schemaAlgebra <- schemaRegistry
      kafkaClientAlgebra <- KafkaClientAlgebra.test[IO]
      metadataAlgebra <- MetadataAlgebra.test[IO]
      expectedDeletedV1Topics <- IO.pure(getExpectedDeletedTopics(v1TopicNames, topicNamesToDelete, kafkaTopicNamesToFail))
      expectedDeletedV2Topics <- IO.pure(getExpectedDeletedTopics(v2TopicNames, topicNamesToDelete, kafkaTopicNamesToFail))
      _ <- writeV2TopicMetadata(v2TopicNames, metadataAlgebra)
      // create all topics
      _ <- (v1TopicNames++v2TopicNames).traverse(topic => kafkaAdmin.createTopic(topic,TopicDetails(1,1)))
      // register all topics
      _ <- registerTopics(v1TopicNames, schemaAlgebra, registerKey, upgrade=false)
      _ <- registerTopics(v2TopicNames, schemaAlgebra, registerKey, upgrade=false)
      // upgrade any topics needed
      _ <- registerTopics(List(upgradeTopic._1), schemaAlgebra, registerKey, upgradeTopic._2)
      // delete all given topics
      errors <-  new TopicDeletionProgram[IO](
            kafkaAdmin,
            kafkaClientAlgebra,
            Subject.createValidated(v2MetadataTopicName).get,
            v1MetadataTopicName,
            schemaAlgebra,
            metadataAlgebra
          ).deleteTopic(topicNamesToDelete)
      // get all topic names
      allTopics <- kafkaAdmin.getTopicNames
      // get all versions of any given topic
      allSchemas <- possibleFailureGetAllSchemaVersions(schemasToSucceed, schemaAlgebra)
      v1Messages <- kafkaClientAlgebra.consumeStringKeyMessages(v1MetadataTopicName, consumerGroup, false).take(expectedDeletedV1Topics.length).compile.toList
      v2Messages <- kafkaClientAlgebra.consumeMessages(v2MetadataTopicName, consumerGroup, false).take(expectedDeletedV2Topics.length).compile.toList
    } yield {
      assertionError(errors)
      allTopics shouldBe (v1TopicNames++v2TopicNames).toSet.diff(topicNamesToDelete.toSet.diff(kafkaTopicNamesToFail.toSet)).toList
      allSchemas shouldBe allTopics.map(topic => topic + "-value")
      v1Messages shouldBe toV1TombstoneRecords(expectedDeletedV1Topics)
      v2Messages shouldBe toV2TombstoneRecords(expectedDeletedV2Topics)
    }).unsafeRunSync()
  }

  private def toV2TombstoneRecords(topicNames: List[String]) = {
    topicNames.map(topic => TopicMetadataV2Key.codec.encode(TopicMetadataV2Key(Subject.createValidated(topic).get)) match {
      case Right(x) => (x, None, None)
    })
  }

  private def toV1TombstoneRecords(topicNames: List[String]) = {
    topicNames.map(topic => (topic.some, None, None))
  }

  private def applyGoodTestcase(v1TopicNames: List[String],
                                v2TopicNames: List[String],
                                topicNamesToDelete: List[String],
                                registerKey: Boolean = false,
                                upgradeTopic: Tuple2[String, Boolean] = ("", false),
                                assertionError: ErrorChecker = _ => ()): Unit = {
    applyTestcase(KafkaAdminAlgebra.test[IO], SchemaRegistry.test[IO],
      v1TopicNames, v2TopicNames, topicNamesToDelete, v1TopicNames++v2TopicNames, registerKey,
      List.empty, upgradeTopic, assertionError)
  }


  it should "Delete a Single Topic from Kafka value only" in {
    applyGoodTestcase(List("topic1"), List.empty, List("topic1"))
  }

  it should "Delete a Single Topic from Multiple topics in Kafka value only" in {
    applyGoodTestcase(twoTopics, List.empty, List("topic1"))
  }

  it should "Delete Multiple Topics from Kafka value only" in {
    applyGoodTestcase(twoTopics, List.empty, twoTopics)
  }

  it should "Delete a Single Topic from Multiple topics in Kafka key and value" in {
    applyGoodTestcase(twoTopics, List.empty, List("topic1"), registerKey = true)
  }

  it should "Delete a single v2 topic" in {
    applyGoodTestcase(List.empty, List("_topic1.name", "_topic2.name"), List("_topic1.name"), registerKey = true)
  }

  it should "Delete multiple v2 topics" in {
    applyGoodTestcase(List.empty, List("_topic1.name", "_topic2.name"), List("_topic1.name", "_topic2.name"), registerKey = true)
  }

  it should "Delete a v1 and v2 topic" in {
    applyGoodTestcase(twoTopics, List("_topic1.name", "_topic2.name"), List("_topic1.name", "_topic2.name"), registerKey = true)
  }

  it should "Delete Multiple Topics from Kafka key and value" in {
    applyGoodTestcase(twoTopics, List.empty, twoTopics, registerKey = true)
  }

  it should "Return a KafkaDeletionError if the topic does not exist" in {
    applyGoodTestcase(twoTopics, List.empty, List("topic3"), registerKey = true, noUpgrade, invalidErrorChecker)
  }

  it should "Delete nothing from Kafka or SchemaRegistry with an empty list" in {
    applyGoodTestcase(twoTopics, List.empty, List(), registerKey = true)
  }

  it should "Delete multiple versions of a schema" in {
    applyGoodTestcase(twoTopics, List.empty, List("topic1"), registerKey = true, ("topic1", true))
  }

  // FAILURE CASES
  it should "Return a KafkaDeletionError if the topic fails to delete" in {
    applyTestcase(kafkabadTest[IO], SchemaRegistry.test[IO],
      v1TopicNames = twoTopics, v2TopicNames = List(), topicNamesToDelete = twoTopics,
      registerKey = true, kafkaTopicNamesToFail = twoTopics,
      schemasToSucceed = twoTopics, assertionError = invalidErrorChecker)
  }

  it should "Return a SchemaDeletionError if deleting schemas fails" in {
    applyTestcase(KafkaAdminAlgebra.test[IO], schemaBadTest[IO](true),
      v1TopicNames = twoTopics, v2TopicNames = List(), topicNamesToDelete = List("topic1"),
      registerKey = true, kafkaTopicNamesToFail = List.empty,
      schemasToSucceed = List("topic2"), assertionError = invalidErrorChecker)
  }

}
