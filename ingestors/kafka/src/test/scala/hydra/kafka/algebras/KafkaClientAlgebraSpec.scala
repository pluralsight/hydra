package hydra.kafka.algebras

import akka.actor.ActorSystem
import cats.effect.{ContextShift, IO, Timer}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.generic.GenericRecord
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import vulcan.Codec
import vulcan.generic._

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

  implicit private val system: ActorSystem = ActorSystem(
    "kafka-client-spec-system"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EmbeddedKafka.stop()
  }

  (for {
    live <- KafkaClientAlgebra.live[IO](s"localhost:$port")
    test <- KafkaClientAlgebra.test[IO]
  } yield {
    runTest(live)
  }).unsafeRunSync()

  private def runTest(kafkaClient: KafkaClientAlgebra[IO], isTest: Boolean = false): Unit = {
    (if (isTest) "KafkaClient#test" else "KafkaClient#live") must {
      "publish message to fs2.kafka" in {
        val (topic, key, value) = topicAndKeyAndValue("topic1","key1","value1")
        kafkaClient.publishMessage((key, value), topic).map(r => assert(r.isRight))
      }

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
      SimpleCaseClassKey.codec.encode(SimpleCaseClassKey(key)).asInstanceOf[GenericRecord],
      SimpleCaseClassValue.codec.encode(SimpleCaseClassValue(value)).asInstanceOf[GenericRecord])
  }
}
