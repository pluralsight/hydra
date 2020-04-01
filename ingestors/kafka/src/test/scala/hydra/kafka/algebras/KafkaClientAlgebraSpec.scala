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

  private val port = 8023

  implicit private val kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = port, zooKeeperPort = 3027)

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
//    runTest(schemaRegistryAlgebra, live)
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
        //        val records2 = kafkaClient.consumeMessages(topic, "secondConsumerGroup").take(1).compile.toList.unsafeRunSync()
        //        records2 should contain allOf((key,value), (key2,value2))
        val records = kafkaClient.consumeMessages(topic, "newConsumerGroup6").take(2).compile.toList.unsafeRunSync()
        records should contain allOf((key2, value2), (key, value))
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
