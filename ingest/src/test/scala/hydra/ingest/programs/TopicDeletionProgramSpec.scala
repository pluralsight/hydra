package hydra.ingest.programs

import cats.effect.IO
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.KafkaAdminAlgebra
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.apache.avro.SchemaBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import cats.implicits._

class TopicDeletionProgramSpec extends AnyFlatSpec with Matchers {

  private def applyTestcase(kafkaAdminAlgebra: IO[KafkaAdminAlgebra[IO]],
                           schemaRegistry: IO[SchemaRegistry[IO]],
                            topicNames: List[String],
                            topicNamesToDelete: List[String],
                            registerKey: Boolean): Unit = {
    (for {
      kafkaAlgebra <- kafkaAdminAlgebra
      schemaAlgebra <- schemaRegistry
      _ <- topicNames.traverse(topic => kafkaAlgebra.createTopic(topic,TopicDetails(1,1)))
      _ <- topicNames.flatMap(topic => if(registerKey) List(topic + "-key", topic + "-value") else List(topic + "-value")).traverse(topic =>
        schemaAlgebra.registerSchema(topic,
          SchemaBuilder.record("name" + topic.replace("-",""))
            .fields().requiredString("id" + topic.replace("-","")).endRecord()))
      _ <-  new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra).deleteTopic(topicNamesToDelete)
      allTopics <- kafkaAlgebra.getTopicNames
      allSchemas <- topicNames.traverse(topic => schemaAlgebra.getAllVersions(topic + "-value").map(versions => if(versions.nonEmpty) Some(topic + "-value") else None)).map(_.flatten)
    } yield {
      allTopics shouldBe topicNames.toSet.diff(topicNamesToDelete.toSet).toList
      allSchemas shouldBe allTopics.map(topic => topic + "-value")
    }).unsafeRunSync()
  }

  it should "Delete Single Topic from Kafka value only" in {
    applyTestcase(KafkaAdminAlgebra.test[IO], SchemaRegistry.test[IO], List("topic1"), List("topic1"), registerKey = false)
  }

  it should "Delete Single Topic from Multiple topics in Kafka value only" in {
    applyTestcase(KafkaAdminAlgebra.test[IO], SchemaRegistry.test[IO], List("topic1","topic2"), List("topic1"), registerKey = false)
  }

  it should "Delete Single Topic from Multiple topics in Kafka key and value" in {
    applyTestcase(KafkaAdminAlgebra.test[IO], SchemaRegistry.test[IO], List("topic1","topic2"), List("topic1"), registerKey = true)
  }

  it should "Delete Multiple Topics from Kafka value only" in {
    applyTestcase(KafkaAdminAlgebra.test[IO], SchemaRegistry.test[IO], List("topic1", "topic2"), List("topic1","topic2"), registerKey = false)
  }

  it should "Delete Multiple Topics from Kafka key and value" in {
    applyTestcase(KafkaAdminAlgebra.test[IO], SchemaRegistry.test[IO], List("topic1", "topic2"), List("topic1","topic2"), registerKey = true)
  }

}
