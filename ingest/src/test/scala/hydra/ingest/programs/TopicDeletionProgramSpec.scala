package hydra.ingest.programs

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.concurrent.Ref
import cats.effect.{IO, Sync}
import hydra.avro.registry.{SchemaRegistry, SchemaRegistryException}
import hydra.kafka.algebras.KafkaAdminAlgebra
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import cats.implicits._
import hydra.avro.registry.SchemaRegistry.{IncompatibleSchemaException, SchemaId, SchemaVersion}
import hydra.kafka.algebras.KafkaAdminAlgebra.{KafkaDeleteTopicError, KafkaDeleteTopicErrorList, LagOffsets, Offset, Topic, TopicAndPartition, TopicName}
import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException

class TopicDeletionProgramSpec extends AnyFlatSpec with Matchers {

  def schemaBadTest[F[_]: Sync]: F[SchemaRegistry[F]] = Sync[F].delay {
    getFromBadSchemaRegistryClient(new MockSchemaRegistryClient)
  }

  private def getFromBadSchemaRegistryClient[F[_]: Sync](schemaRegistryClient: SchemaRegistryClient): SchemaRegistry[F] =
    new SchemaRegistry[F] {

      private implicit class CheckKeySchemaEvolution(schemasF: F[List[Schema]]) {
        Sync[F].raiseError(IncompatibleSchemaException(s"Key schema evolutions are not permitted."))
      }

      override def registerSchema(subject: String,schema: Schema): F[SchemaId] = {
        Sync[F].delay(0)
      }

      override def deleteSchemaOfVersion(subject: String,version: SchemaVersion): F[Unit] =
        Sync[F].raiseError(new SchemaRegistryException(new Exception(s"Unable to delete $version of $subject"), subject))

      override def getVersion(subject: String,schema: Schema): F[SchemaVersion] =
        Sync[F].raiseError(new Exception(s"Unable to get version for $subject"))

      override def getAllVersions(subject: String): F[List[SchemaId]] =
        Sync[F].raiseError(new SchemaRegistryException(new Exception(s"Unable to get all versions of $subject"), subject))

      override def getAllSubjects: F[List[String]] =
        Sync[F].raiseError(new Exception("Unable to get all Subjects"))

      override def getSchemaRegistryClient: F[SchemaRegistryClient] = Sync[F].pure(schemaRegistryClient)

      //TODO: Test this
      override def getLatestSchemaBySubject(subject: String): F[Option[Schema]] = Sync[F].raiseError(new Exception("LatestSchemaBySubject failed"))

      override def getSchemaFor(subject: String, schemaVersion: SchemaVersion): F[Option[Schema]] =
        Sync[F].raiseError(new Exception(s"getSchemaFor failed for version $schemaVersion of $subject"))

    }

  def kafkabadTest[F[_]: Sync]: F[KafkaAdminAlgebra[F]] =
    Ref[F].of(Map[TopicName, Topic]()).flatMap(getBadTestKafkaClient[F])

  private[this] def getBadTestKafkaClient[F[_]: Sync](ref: Ref[F, Map[TopicName, Topic]]): F[KafkaAdminAlgebra[F]] = Sync[F].delay  {
    new KafkaAdminAlgebra[F] {
      override def describeTopic(name: TopicName): F[Option[Topic]] =
      // return None
        ref.get.map(_ => None)

      override def getTopicNames: F[List[TopicName]] =
      // return empty list
        ref.get.map(_ => List())

      override def createTopic(
                                name: TopicName,
                                details: TopicDetails
                              ): F[Unit] = {
        ref.update(old => old)
      }

      override def deleteTopic(name: String): F[Unit] =
        Sync[F].raiseError(new Exception("An exception has occured in your deleteTopic"))

      // This is intentionally unimplemented. This test class has no way of obtaining this offset information.
      override def getConsumerGroupOffsets(consumerGroup: String): F[Map[TopicAndPartition, Offset]] = ???
      // This is intentionally unimplemented. This test class has no way of obtaining this offset information.
      override def getLatestOffsets(topic: TopicName): F[Map[TopicAndPartition, Offset]] = ???
      // This is intentionally unimplemented. This test class has no way of obtaining this offset information.
      override def getConsumerLag(topic: TopicName, consumerGroup: String): F[Map[TopicAndPartition, LagOffsets]] = ???

      override def kafkaContainsTopic(name: TopicName): F[Boolean] =
        getTopicNames.map(topics => false)

      override def deleteTopics(topicNames: List[String]): F[Either[KafkaDeleteTopicErrorList, Unit]] =
        Sync[F].pure(Left(new KafkaDeleteTopicErrorList( NonEmptyList.fromList(
          topicNames.map(topic => KafkaDeleteTopicError(topic, new Exception("Unable to delete topic")))).get)))
    }
  }

  private type ErrorChecker = ValidatedNel[DeleteTopicError, Unit] => Unit

  private def applyTestcase(kafkaAdminAlgebra: IO[KafkaAdminAlgebra[IO]],
                           schemaRegistry: IO[SchemaRegistry[IO]],
                            topicNames: List[String],
                            topicNamesToDelete: List[String],
                            registerKey: Boolean,
                            topicNamesToFail: List[String] = List.empty,
                            assertionError: ErrorChecker = _ => ()): Unit = {
    (for {
      kafkaAlgebra <- kafkaAdminAlgebra
      schemaAlgebra <- schemaRegistry
      // create all topics
      _ <- topicNames.traverse(topic => kafkaAlgebra.createTopic(topic,TopicDetails(1,1)))
      // register all topics
      _ <- topicNames.flatMap(topic => if(registerKey) List(topic + "-key", topic + "-value") else List(topic + "-value")).traverse(topic =>
        schemaAlgebra.registerSchema(topic,
          SchemaBuilder.record("name" + topic.replace("-",""))
            .fields().requiredString("id" + topic.replace("-","")).endRecord()))
      // delete all given topics to delete
      errors <-  new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra).deleteTopic(topicNamesToDelete)
      allTopics <- kafkaAlgebra.getTopicNames
      allSchemas <- topicNames.traverse(topic => schemaAlgebra.getAllVersions(topic + "-value").map(versions => if(versions.nonEmpty) Some(topic + "-value") else None)).map(_.flatten)
    } yield {
      assertionError(errors)
      allTopics shouldBe topicNames.toSet.diff(topicNamesToDelete.toSet).toList
      allSchemas shouldBe allTopics.map(topic => topic + "-value")
    }).unsafeRunSync()
  }

  it should "Delete a Single Topic from Kafka value only" in {
    applyTestcase(KafkaAdminAlgebra.test[IO], SchemaRegistry.test[IO], List("topic1"), List("topic1"), registerKey = false)
  }

  it should "Delete a Single Topic from Multiple topics in Kafka value only" in {
    applyTestcase(KafkaAdminAlgebra.test[IO], SchemaRegistry.test[IO], List("topic1","topic2"), List("topic1"), registerKey = false)
  }

  it should "Delete Multiple Topics from Kafka value only" in {
    applyTestcase(KafkaAdminAlgebra.test[IO], SchemaRegistry.test[IO], List("topic1", "topic2"), List("topic1","topic2"), registerKey = false)
  }

  it should "Delete a Single Topic from Multiple topics in Kafka key and value" in {
    applyTestcase(KafkaAdminAlgebra.test[IO], SchemaRegistry.test[IO], List("topic1","topic2"), List("topic1"), registerKey = true)
  }

  it should "Delete Multiple Topics from Kafka key and value" in {
    applyTestcase(KafkaAdminAlgebra.test[IO], SchemaRegistry.test[IO], List("topic1", "topic2"), List("topic1","topic2"), registerKey = true)
  }

  it should "Return a KafkaDeletionError if the topic does not exist" in {
    val checkErrors: ErrorChecker = errors => errors shouldBe a [Invalid[_]]
    applyTestcase(KafkaAdminAlgebra.test[IO], SchemaRegistry.test[IO], List("topic1", "topic2"), List("topic3"), registerKey = true, List.empty, checkErrors)
  }

  it should "Delete nothing from Kafka or SchemaRegistry with an empty list" in {
    applyTestcase(KafkaAdminAlgebra.test[IO], SchemaRegistry.test[IO], List("topic1", "topic2"), List(), registerKey = true)
  }

  it should "Return a SchemaDeletionError if getting all versions fail" in {
    val checkErrors: ErrorChecker = errors => errors shouldBe a [Invalid[_]]
    applyTestcase(kafkabadTest[IO], schemaBadTest[IO], List("topic1", "topic2"), List("topic1"), registerKey = true, List.empty, checkErrors)
  }

  it should "Return a SchemaDeletionError if deleting a specific version fails" in {

  }

  it should "Return a KafkaDeletionError if the topic fails to delete" in {
    val checkErrors: ErrorChecker = errors => errors shouldBe a [Invalid[_]]
    applyTestcase(kafkabadTest[IO], SchemaRegistry.test[IO], List("topic1", "topic2"), List("topic1", "topic2"), registerKey = true, List("topic1", "topic2"), checkErrors)
  }

}
