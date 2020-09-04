package hydra.ingest.programs

import cats.effect.IO
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.KafkaAdminAlgebra
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.apache.avro.SchemaBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TopicDeletionProgramSpec extends AnyFlatSpec with Matchers {
  it should "Delete Topics from Kafka value only" in {
    (for {
      kafkaAlgebra <- KafkaAdminAlgebra.test[IO]
      schemaAlgebra <- SchemaRegistry.test[IO]
      _ <- kafkaAlgebra.createTopic("test1", TopicDetails(1,1))
      _ <- schemaAlgebra.registerSchema("test1-value", SchemaBuilder.record("name").fields().requiredString("id").endRecord())
      _ <- new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra).deleteTopic(List("test1"))
      finalTopic <- kafkaAlgebra.describeTopic("test1")
      finalSchema <- schemaAlgebra.getSchemaFor("test1-value",1)
    } yield {
      finalTopic shouldBe None
      finalSchema shouldBe None
    }).unsafeRunSync()
  }

  it should "Delete Topics from Kafka key and value" in {
    (for {
      kafkaAlgebra <- KafkaAdminAlgebra.test[IO]
      schemaAlgebra <- SchemaRegistry.test[IO]
      _ <- kafkaAlgebra.createTopic("test1", TopicDetails(1,1))
      _ <- schemaAlgebra.registerSchema("test1-key", SchemaBuilder.record("name").fields().requiredString("id").endRecord())
      _ <- schemaAlgebra.registerSchema("test1-value", SchemaBuilder.record("name").fields().requiredString("id").endRecord())
      _ <- new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra).deleteTopic(List("test1"))
      finalTopic <- kafkaAlgebra.describeTopic("test1")
      finalValueSchema <- schemaAlgebra.getSchemaFor("test1-value",1)
      finalKeySchema <- schemaAlgebra.getSchemaFor("test1-key",1)
    } yield {
      finalTopic shouldBe None
      finalKeySchema shouldBe None
      finalValueSchema shouldBe None
    }).unsafeRunSync()
  }

  it should "Delete Multiple Topics from Kafka value only" in {
    (for {
      kafkaAlgebra <- KafkaAdminAlgebra.test[IO]
      schemaAlgebra <- SchemaRegistry.test[IO]
      _ <- kafkaAlgebra.createTopic("test1", TopicDetails(1,1))
      _ <- kafkaAlgebra.createTopic("test2", TopicDetails(1,1))
      _ <- schemaAlgebra.registerSchema("test1-value", SchemaBuilder.record("name").fields().requiredString("id").endRecord())
      _ <- schemaAlgebra.registerSchema("test2-value", SchemaBuilder.record("name2").fields().requiredString("id2").endRecord())
      _ <- new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra).deleteTopic(List("test1", "test2"))
      finalTopicOne <- kafkaAlgebra.describeTopic("test1")
      finalTopicTwo <- kafkaAlgebra.describeTopic("test2")
      finalSchemaOne <- schemaAlgebra.getSchemaFor("test1-value",1)
      finalSchemaTwo <- schemaAlgebra.getSchemaFor("test2-value",1)
    } yield {
      finalTopicOne shouldBe None
      finalSchemaOne shouldBe None
      finalTopicTwo shouldBe None
      finalSchemaTwo shouldBe None
    }).unsafeRunSync()
  }

  it should "Delete Multiple Topics from Kafka key and value" in {
    (for {
      kafkaAlgebra <- KafkaAdminAlgebra.test[IO]
      schemaAlgebra <- SchemaRegistry.test[IO]
      _ <- kafkaAlgebra.createTopic("test1", TopicDetails(1,1))
      _ <- kafkaAlgebra.createTopic("test2", TopicDetails(1,1))
      _ <- schemaAlgebra.registerSchema("test1-key", SchemaBuilder.record("name").fields().requiredString("id").endRecord())
      _ <- schemaAlgebra.registerSchema("test1-value", SchemaBuilder.record("name").fields().requiredString("id").endRecord())
      _ <- schemaAlgebra.registerSchema("test2-key", SchemaBuilder.record("name2").fields().requiredString("id2").endRecord())
      _ <- schemaAlgebra.registerSchema("test2-value", SchemaBuilder.record("name2").fields().requiredString("id2").endRecord())
      _ <- new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra).deleteTopic(List("test1", "test2"))
      finalTopicOne <- kafkaAlgebra.describeTopic("test1")
      finalTopicTwo <- kafkaAlgebra.describeTopic("test2")
      finalSchemaKeyOne <- schemaAlgebra.getSchemaFor("test1-key",1)
      finalSchemaKeyTwo <- schemaAlgebra.getSchemaFor("test2-key",1)
      finalSchemaValueOne <- schemaAlgebra.getSchemaFor("test1-value",1)
      finalSchemaValueTwo <- schemaAlgebra.getSchemaFor("test2-value",1)
    } yield {
      finalTopicOne shouldBe None
      finalSchemaKeyOne shouldBe None
      finalSchemaValueOne shouldBe None
      finalTopicTwo shouldBe None
      finalSchemaKeyTwo shouldBe None
      finalSchemaValueTwo shouldBe None
    }).unsafeRunSync()
  }

  it should "Delete Only Topic listed from Kafka value only" in {
    (for {
      kafkaAlgebra <- KafkaAdminAlgebra.test[IO]
      schemaAlgebra <- SchemaRegistry.test[IO]
      _ <- kafkaAlgebra.createTopic("test1", TopicDetails(1,1))
      _ <- kafkaAlgebra.createTopic("test2", TopicDetails(1,1))
      _ <- schemaAlgebra.registerSchema("test1-value", SchemaBuilder.record("name").fields().requiredString("id").endRecord())
      _ <- schemaAlgebra.registerSchema("test2-value", SchemaBuilder.record("name2").fields().requiredString("id2").endRecord())
      _ <- new TopicDeletionProgram[IO](kafkaAlgebra, schemaAlgebra).deleteTopic(List("test1"))
      finalTopicOne <- kafkaAlgebra.describeTopic("test1")
      finalTopicTwo <- kafkaAlgebra.kafkaContainsTopic("test2")
      finalSchemaOne <- schemaAlgebra.getSchemaFor("test1-value",1)
      finalSchemaTwo <- schemaAlgebra.getSchemaFor("test2-value",1)
    } yield {
      finalTopicOne shouldBe None
      finalSchemaOne shouldBe None
      finalTopicTwo shouldBe true
      finalSchemaTwo shouldBe Some(SchemaBuilder.record("name2").fields().requiredString("id2").endRecord())
    }).unsafeRunSync()
  }

}
