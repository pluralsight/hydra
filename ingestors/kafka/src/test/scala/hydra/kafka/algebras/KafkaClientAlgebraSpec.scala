package hydra.kafka.algebras

import cats.effect.{ContextShift, IO, Timer}
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
  }).unsafeRunSync()

  private def runTest(schemaRegistry: SchemaRegistry[IO], kafkaClient: KafkaClientAlgebra[IO], isTest: Boolean = false): Unit = {
    (if (isTest) "KafkaClient#test" else "KafkaClient#live") must {
      "publish message to kafka" in {
        val (topic, key, value) = topicAndKeyAndValue("topic1","key1","value1")
        (schemaRegistry.registerSchema(subject = s"$topic-key", key.getSchema) *>
          schemaRegistry.registerSchema(subject = s"$topic-value", value.getSchema) *>
          schemaRegistry.getAllSubjects.map(_.foreach(println)) *>
        kafkaClient.publishMessage((key, value), topic).map{ r =>
          println(r)
          assert(r.isRight)}).unsafeRunSync()
      }

      "consume message from kafka" in {
        val (topic, key, value) = topicAndKeyAndValue("topic1","key1","value1")
        val records = kafkaClient.consumeMessages(topic,"newConsumerGroup").compile.toList.unsafeRunSync()
        records should have length 1
        records.head shouldBe (key, value)
      }

//      "consume nothing from kafka" in {
//        kafkaClient.consumeMessages("unknownTopic","newConsumerGroup").compile.toList.map(l => assert(l.length == 100)).unsafeRunSync()
//      }

//      "handle ingestor timeout" in {
//        testCase(IngestorTimeout, Left(PublishError.Timeout))
//      }
//
//      "handle unknown responses" in {
//        testCase(
//          RequestPublished,
//          Left(PublishError.UnexpectedResponse(RequestPublished))
//        )
//      }
//
//      "handle ingestor error" in {
//        val exception = new Exception("Error")
//        testCase(
//          IngestorError(exception),
//          Left(PublishError.Failed(exception))
//        )
//      }
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
