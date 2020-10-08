package hydra.ingest.programs

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.{IO, Sync}
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.KafkaAdminAlgebra
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import cats.implicits._
import hydra.avro.registry.SchemaRegistry.{SchemaId, SchemaVersion}
import hydra.kafka.algebras.KafkaAdminAlgebra.{KafkaDeleteTopicError, KafkaDeleteTopicErrorList, LagOffsets, Offset, Topic, TopicAndPartition, TopicName}
import io.confluent.kafka.schemaregistry.client.{SchemaRegistryClient}

class TopicDeletionProgramSpec extends AnyFlatSpec with Matchers {

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

  private def buildSchema(topic: String, upgrade: Boolean): Schema = {
    val schemaStart = SchemaBuilder.record("name" + topic.replace("-", ""))
      .fields().requiredString("id" + topic.replace("-", ""))
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

  private def applyTestcase(kafkaAdminAlgebra: IO[KafkaAdminAlgebra[IO]],
                            schemaRegistry: IO[SchemaRegistry[IO]],
                            topicNames: List[String],
                            topicNamesToDelete: List[String],
                            schemasToSucceed: List[String] = List.empty,
                            registerKey: Boolean,
                            kafkaTopicNamesToFail: List[String] = List.empty,
                            upgradeTopic: Tuple2[String, Boolean] = ("", false),
                            assertionError: ErrorChecker = _ => ()): Unit = {
    (for {
      kafkaAlgebra <- kafkaAdminAlgebra
      schemaAlgebra <- schemaRegistry
      // create all topics
      _ <- topicNames.traverse(topic => kafkaAlgebra.createTopic(topic,TopicDetails(1,1)))
      // register all topics
      _ <- registerTopics(topicNames, schemaAlgebra, registerKey, upgrade=false)
      // upgrade any topics needed
      _ <- registerTopics(List(upgradeTopic._1), schemaAlgebra, registerKey, upgradeTopic._2)
      // delete all given topics
      errors <-  new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra).deleteTopic(topicNamesToDelete)
      // get all topic names
      allTopics <- kafkaAlgebra.getTopicNames
      // get all versions of any given topic
      allSchemas <- possibleFailureGetAllSchemaVersions(schemasToSucceed, schemaAlgebra)
    } yield {
      assertionError(errors)
      allTopics shouldBe topicNames.toSet.diff(topicNamesToDelete.toSet.diff(kafkaTopicNamesToFail.toSet)).toList
      allSchemas shouldBe allTopics.map(topic => topic + "-value")
    }).unsafeRunSync()
  }

  private def applyGoodTestcase(topicNames: List[String],
                                topicNamesToDelete: List[String],
                                registerKey: Boolean = false,
                                upgradeTopic: Tuple2[String, Boolean] = ("", false),
                                assertionError: ErrorChecker = _ => ()): Unit = {
    applyTestcase(KafkaAdminAlgebra.test[IO], SchemaRegistry.test[IO],
      topicNames, topicNamesToDelete, topicNames, registerKey,
      List.empty, upgradeTopic, assertionError)
  }


  it should "Delete a Single Topic from Kafka value only" in {
    applyGoodTestcase(List("topic1"), List("topic1"))
  }

  it should "Delete a Single Topic from Multiple topics in Kafka value only" in {
    applyGoodTestcase(twoTopics, List("topic1"))
  }

  it should "Delete Multiple Topics from Kafka value only" in {
    applyGoodTestcase(twoTopics, twoTopics)
  }

  it should "Delete a Single Topic from Multiple topics in Kafka key and value" in {
    applyGoodTestcase(twoTopics, List("topic1"), registerKey = true)
  }

  it should "Delete Multiple Topics from Kafka key and value" in {
    applyGoodTestcase(twoTopics, twoTopics, registerKey = true)
  }

  it should "Return a KafkaDeletionError if the topic does not exist" in {
    applyGoodTestcase(twoTopics, List("topic3"), registerKey = true, noUpgrade, invalidErrorChecker)
  }

  it should "Delete nothing from Kafka or SchemaRegistry with an empty list" in {
    applyGoodTestcase(twoTopics, List(), registerKey = true)
  }

  it should "Delete multiple versions of a schema" in {
    applyGoodTestcase(twoTopics, List("topic1"), registerKey = true, ("topic1", true))
  }

  // FAILURE CASES
  it should "Return a KafkaDeletionError if the topic fails to delete" in {
    applyTestcase(kafkabadTest[IO], SchemaRegistry.test[IO],
      topicNames = twoTopics, topicNamesToDelete = twoTopics,
      registerKey = true, kafkaTopicNamesToFail = twoTopics,
      schemasToSucceed = twoTopics, assertionError = invalidErrorChecker)
  }

  it should "Return a SchemaDeletionError if deleting schemas fails" in {
    applyTestcase(KafkaAdminAlgebra.test[IO], schemaBadTest[IO](true),
      topicNames = twoTopics, topicNamesToDelete = List("topic1"),
      registerKey = true, kafkaTopicNamesToFail = List.empty,
      schemasToSucceed = List("topic2"), assertionError = invalidErrorChecker)
  }

}
