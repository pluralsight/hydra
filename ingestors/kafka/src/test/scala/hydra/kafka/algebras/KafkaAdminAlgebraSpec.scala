package hydra.kafka.algebras

import akka.actor.ActorSystem
import cats.effect.{Clock, ContextShift, IO, Sync, Timer}
import hydra.kafka.algebras.KafkaAdminAlgebra.{LagOffsets, Offset, Topic, TopicAndPartition}
import hydra.avro.registry.SchemaRegistry
import hydra.common.config.KafkaConfigUtils
import hydra.kafka.algebras.KafkaAdminAlgebra.{LagOffsets, Offset, TopicAndPartition}
import hydra.kafka.algebras.KafkaClientAlgebra.getOptionalGenericRecordDeserializer
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.joda.time.DurationFieldType.seconds
import org.scalatest.{BeforeAndAfterAll, stats}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.MILLISECONDS

final class KafkaAdminAlgebraSpec
    extends AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with EmbeddedKafka {

  private val port = 8023

  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] =
    Slf4jLogger.getLogger[F]

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

  private val bootstrapServers = s"lkaclkc-221192-6m08zp.us-west-2.aws.glb.confluent.cloud:9092"
  private val topicName = "Topic1-local"

  (for {
    live <- KafkaAdminAlgebra
      .live[IO](bootstrapServers, KafkaConfigUtils.kafkaSecurityEmptyConfig)
    test <- KafkaAdminAlgebra.test[IO]()
  } yield {
    runTests(live)
//    runTests(test, isTest = true)
  }).unsafeRunSync()

  private def runTests(kafkaClient: KafkaAdminAlgebra[IO], isTest: Boolean = false): Unit = {
    (if (isTest) "kKafkaAdmin#test" else "kKafkaAdmin#test") must {
//      "kcreate a topic" in {
//        val topicDetails = TopicDetails(3, 3, 1)
//        val clock = Clock[IO]
//        val run = for {
//          start <- clock.monotonic(MILLISECONDS)
//          _ <- kafkaClient.createTopic(topicName, topicDetails) *> kafkaClient
//            .describeTopic(topicName)
//            .map {
//              case Some(topic) =>
//                topic.name shouldBe topicName
//                topic.numberPartitions shouldBe topicDetails.numPartitions
//              case None => fail("Found None when a Topic was Expected")
//            }
//          end <- clock.monotonic(MILLISECONDS)
//        } yield {
//          println(s"Execution time: ${end - start}")
//        }
//
//        run.unsafeRunSync()
//      }
//
//      "list all topics" in {
//        kafkaClient.getTopicNames.unsafeRunSync() shouldBe List(topicName)
//      }
//
//      "validate topic exists" in {
//        val topicCreated = "topic_created"
//        (for {
//          _ <- kafkaClient.createTopic(topicCreated, TopicDetails(1,1,1))
//          maybeTopic <- kafkaClient.describeTopic(topicCreated)
//        } yield maybeTopic shouldBe Some(Topic(topicCreated,1)) ).unsafeRunSync()
//      }
//
//      "delete a topic and describe" in {
//        val topicToDelete = "topic_to_delete"
//        val clock = Clock[IO]
////        implicit val timer= Timer[IO]
//        (for {
//          start <- clock.monotonic(MILLISECONDS)
//          _ <- kafkaClient.createTopic(topicToDelete, TopicDetails(3, 3, 1))
//          finishCreate <- clock.monotonic(MILLISECONDS)
//          _ <- kafkaClient.deleteTopic(topicToDelete)
//          fifnishDelete <-clock.monotonic(MILLISECONDS)
//          maybeTopic <- kafkaClient.describeTopic(topicToDelete)
//        } yield {println(s"CreateTime: ${finishCreate - start} DeleteTime: ${fifnishDelete - finishCreate}"); maybeTopic should not be defined})
//          .timeout(60 seconds).unsafeRunSync()
//      }
//
//      "delete multiple topics" in {
//        val topicsToDelete = List("topic1","topic2","topic3","topic4","topic5")
//        topicsToDelete.map(topic => kafkaClient.createTopic(topic, TopicDetails(1, 1, 1)).unsafeRunSync())
//        kafkaClient.deleteTopics(topicsToDelete).unsafeRunSync()
//        topicsToDelete.map(topic => kafkaClient.describeTopic(topic).unsafeRunSync() shouldBe None)
//      }

//      if (!isTest) {
//
//        import fs2.kafka._
//        val consumerGroup = "testconsumergroup"
//
//        def produceTest(): Unit = {
//          val producerSettings = ProducerSettings[IO, String, String](
//            keySerializer = Serializer[IO, String],
//            valueSerializer = Serializer[IO, String]
//          ).withBootstrapServers(bootstrapServers)
//          val record = ProducerRecord[String, String](topicName, "key", "value")
//          fs2.Stream.eval(IO.pure(ProducerRecords.one(record)))
//            .through(produce(producerSettings))
//            .compile.drain.unsafeRunSync()
//        }
//
//        def consumeTest(): Unit = {
//          val consumerSettings: ConsumerSettings[IO, String, String] = ConsumerSettings(
//            keyDeserializer = Deserializer[IO, String],
//            valueDeserializer = Deserializer[IO, String]
//          )
//            .withAutoOffsetReset(AutoOffsetReset.Earliest)
//            .withBootstrapServers(bootstrapServers)
//            .withGroupId(consumerGroup)
//          consumerStream(consumerSettings)
//            .evalTap(_.subscribeTo(topicName))
//            .flatMap(_.stream)
//            .evalTap(_.offset.commit)
//            .take(1)
//            .compile
//            .toList
//            .unsafeRunSync()
//        }
//
//        "get group offsets" in {
//          produceTest()
//          consumeTest()
//          val consumerOffsets = kafkaClient.getConsumerGroupOffsets(consumerGroup).unsafeRunSync()
//          consumerOffsets shouldBe Map(TopicAndPartition(topicName,1) -> Offset(1))
//        }
//
//        "get latest offsets" in {
//          val consumerOffsets = kafkaClient.getLatestOffsets(topicName).unsafeRunSync()
//          consumerOffsets shouldBe Map(
//            TopicAndPartition(topicName,0) -> Offset(0),
//            TopicAndPartition(topicName,1) -> Offset(1),
//            TopicAndPartition(topicName,2) -> Offset(0)
//          )
//        }
//
//        "get offset lag" in {
//          val consumerOffsets = kafkaClient.getConsumerLag(topicName, consumerGroup).unsafeRunSync()
//          consumerOffsets shouldBe Map(
//            TopicAndPartition(topicName,0) -> LagOffsets(Offset(0), Offset(0)),
//            TopicAndPartition(topicName,1) -> LagOffsets(Offset(1), Offset(1)),
//            TopicAndPartition(topicName,2) -> LagOffsets(Offset(0), Offset(0))
//          )
//        }
//      }
    }
  }
}
