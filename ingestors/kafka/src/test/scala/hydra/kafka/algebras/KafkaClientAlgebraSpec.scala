package hydra.kafka.algebras

import cats.effect.{Concurrent, ContextShift, IO, Timer}
import hydra.avro.registry.SchemaRegistry
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.generic.GenericRecord
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import vulcan.Codec
import vulcan.generic._
import cats.implicits._

import scala.concurrent.ExecutionContext

class KafkaClientAlgebraSpec
  extends AnyWordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with EmbeddedKafka {

  import KafkaClientAlgebraSpec._

  private val port = 8092

  implicit private val kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = port, zooKeeperPort = 3182)

  implicit private val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect
  implicit private val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EmbeddedKafka.stop()
  }


  (for {
    schemaRegistryAlgebra <- SchemaRegistry.test[IO]
    live <- KafkaClientAlgebra.live[IO](s"localhost:$port", schemaRegistryAlgebra)
    test <- KafkaClientAlgebra.test[IO]
  } yield {
    runTest(schemaRegistryAlgebra, live)
    runTest(schemaRegistryAlgebra, test, isTest = true)
  }).unsafeRunSync()

  private def runTest(schemaRegistry: SchemaRegistry[IO], kafkaClient: KafkaClientAlgebra[IO], isTest: Boolean = false): Unit = {
    (if (isTest) "KafkaClient#test" else "KafkaClient#live") must {
      "publish message to kafka" in {
        val (topic, key, value) = topicAndKeyAndValue("topic1","key1","value1")
        (schemaRegistry.registerSchema(subject = s"$topic-key", key.getSchema) *>
          schemaRegistry.registerSchema(subject = s"$topic-value", value.getSchema) *>
        kafkaClient.publishMessage((key, value), topic).map{ r =>
          assert(r.isRight)}).unsafeRunSync()
      }

      val (topic, key, value) = topicAndKeyAndValue("topic1","key1","value1")
      "consume message from kafka" in {
        val records = kafkaClient.consumeMessages(topic,"newConsumerGroup").take(1).compile.toList.unsafeRunSync()
        records should have length 1
        records.head shouldBe (key, value)
      }

      val (_, key2, value2) = topicAndKeyAndValue("topic1","key2","value2")
      "publish a record to existing topic and consume only that value in existing consumer group" in {
        kafkaClient.publishMessage((key2, value2), topic).unsafeRunSync()
        val records = kafkaClient.consumeMessages(topic, "newConsumerGroup6").take(2).compile.toList.unsafeRunSync()
        records should contain allOf((key2, value2), (key, value))
      }

      val (_, key3, value3) = topicAndKeyAndValue("topic1","key3","value3")
      "continue consuming messages from the same stream" in {
        val stream = kafkaClient.consumeMessages(topic, "doesNotReallyMatter")
        stream.take(2).compile.toList.unsafeRunSync() should contain allOf((key2, value2), (key, value))
        kafkaClient.publishMessage((key3, value3), topic).unsafeRunSync()
        stream.take(3).compile.toList.unsafeRunSync().last shouldBe (key3, value3)
      }

      val (topic2, key4, value4) = topicAndKeyAndValue("topic2","key4","value4")
      val (_, key5, value5) = topicAndKeyAndValue("topic2","key5","value5")
      "consume from two different topics" in {
        (schemaRegistry.registerSchema(subject = s"$topic2-key", key4.getSchema) *>
          schemaRegistry.registerSchema(subject = s"$topic2-value", value4.getSchema) *>
          kafkaClient.publishMessage((key4, value4), topic2) *>
          kafkaClient.publishMessage((key5, value5), topic2)).unsafeRunSync()
        val topicOneStream = kafkaClient.consumeMessages(topic, "doesNotReallyMatter")
        val topicTwoStream = kafkaClient.consumeMessages(topic2, "doesNotReallyMatter")

        topicOneStream.take(3).compile.toList.unsafeRunSync() should contain allOf ((key3, value3), (key2, value2), (key, value))
        topicTwoStream.take(2).compile.toList.unsafeRunSync() should contain allOf ((key4, value4), (key5, value5))
      }
    }
  }
}

object KafkaClientAlgebraSpec {

  final case class SimpleCaseClassKey(subject: String)

  object SimpleCaseClassKey {
    implicit val codec: Codec[SimpleCaseClassKey] =
      Codec.derive[SimpleCaseClassKey]
  }

  final case class SimpleCaseClassValue(value: String)

  object SimpleCaseClassValue {
    implicit val codec: Codec[SimpleCaseClassValue] =
      Codec.derive[SimpleCaseClassValue]
  }

  def topicAndKeyAndValue(topic: String, key: String, value: String): (String, GenericRecord, GenericRecord) = {
    (topic,
      SimpleCaseClassKey.codec.encode(SimpleCaseClassKey(key)).map(_.asInstanceOf[GenericRecord]).toOption.get,
      SimpleCaseClassValue.codec.encode(SimpleCaseClassValue(value)).map(_.asInstanceOf[GenericRecord]).toOption.get)
  }
}
