package hydra.kafka.algebras

import akka.actor.ActorSystem
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.KafkaAdminAlgebra.{LagOffsets, Offset, TopicAndPartition}
import hydra.kafka.algebras.KafkaClientAlgebra.getOptionalGenericRecordDeserializer
import hydra.kafka.util.KafkaUtils.TopicDetails
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext

final class KafkaAdminAlgebraSpec
    extends AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with EmbeddedKafka {

  private val port = 8023

  implicit private val kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = port, zooKeeperPort = 3027)

  implicit private val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)

  implicit private val timer: Timer[IO] =
    IO.timer(concurrent.ExecutionContext.global)

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
    live <- KafkaAdminAlgebra
      .live[IO](s"localhost:$port")
    test <- KafkaAdminAlgebra.test[IO]
  } yield {
    runTests(live)
    runTests(test, isTest = true)
  }).unsafeRunSync()

  private def runTests(kafkaClient: KafkaAdminAlgebra[IO], isTest: Boolean = false): Unit = {
    (if (isTest) "KafkaAdmin#test" else "KafkaAdmin#live") must {
      "create a topic" in {
        val topicName = "Topic1"
        val topicDetails = TopicDetails(3, 1.toShort)
        (kafkaClient.createTopic(topicName, topicDetails) *> kafkaClient
          .describeTopic(topicName)
          .map {
            case Some(topic) =>
              topic.name shouldBe topicName
              topic.numberPartitions shouldBe topicDetails.numPartitions
            case None => fail("Found None when a Topic was Expected")
          }).unsafeRunSync()
      }

      "list all topics" in {
        kafkaClient.getTopicNames.unsafeRunSync() shouldBe List("Topic1")
      }

      "delete a topic" in {
        val topicToDelete = "topic_to_delete"
        (for {
          _ <- kafkaClient.createTopic(topicToDelete, TopicDetails(1, 1))
          _ <- kafkaClient.deleteTopic(topicToDelete)
          maybeTopic <- kafkaClient.describeTopic(topicToDelete)
        } yield maybeTopic should not be defined).unsafeRunSync()
      }

      if (!isTest) {

        import fs2.kafka._
        val consumerGroup = "testconsumergroup"

        def produceTest(): Unit = {
          val producerSettings = ProducerSettings[IO, String, String](
            keySerializer = Serializer[IO, String],
            valueSerializer = Serializer[IO, String]
          ).withBootstrapServers(s"localhost:$port")
          val record = ProducerRecord[String, String]("Topic1", "key", "value")
          fs2.Stream.eval(IO.pure(ProducerRecords.one(record)))
            .through(produce(producerSettings))
            .compile.drain.unsafeRunSync()
        }

        def consumeTest(): Unit = {
          val consumerSettings = ConsumerSettings(
            keyDeserializer = Deserializer[IO, String],
            valueDeserializer = Deserializer[IO, String]
          )
            .withAutoOffsetReset(AutoOffsetReset.Earliest)
            .withBootstrapServers(s"localhost:$port")
            .withGroupId(consumerGroup)
          consumerStream(consumerSettings)
            .evalTap(_.subscribeTo("Topic1"))
            .flatMap(_.stream)
            .evalTap(_.offset.commit)
            .take(1)
            .compile
            .toList
            .unsafeRunSync()
        }

        "get group offsets" in {
          produceTest()
          consumeTest()
          val consumerOffsets = kafkaClient.getConsumerGroupOffsets(consumerGroup).unsafeRunSync()
          consumerOffsets shouldBe Map(TopicAndPartition("Topic1",1) -> Offset(1))
        }

        "get latest offsets" in {
          val consumerOffsets = kafkaClient.getLatestOffsets("Topic1").unsafeRunSync()
          consumerOffsets shouldBe Map(
            TopicAndPartition("Topic1",0) -> Offset(0),
            TopicAndPartition("Topic1",1) -> Offset(1),
            TopicAndPartition("Topic1",2) -> Offset(0)
          )
        }

        "get offset lag" in {
          val consumerOffsets = kafkaClient.getConsumerLag("Topic1", consumerGroup).unsafeRunSync()
          consumerOffsets shouldBe Map(
            TopicAndPartition("Topic1",0) -> LagOffsets(Offset(0), Offset(0)),
            TopicAndPartition("Topic1",1) -> LagOffsets(Offset(1), Offset(1)),
            TopicAndPartition("Topic1",2) -> LagOffsets(Offset(0), Offset(0))
          )
        }
      }
    }
  }
}
