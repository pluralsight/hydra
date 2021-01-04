package hydra.kafka.algebras

import java.time.Instant

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, ContextShift, IO, Sync, Timer}
import cats.implicits._
import com.dimafeng.testcontainers.{ForAllTestContainer, KafkaContainer}
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.ConsumerGroupsAlgebra.PartitionOffsetMap
import hydra.kafka.algebras.KafkaClientAlgebra.{OffsetInfo, Record}
import hydra.kafka.model.TopicConsumer.{TopicConsumerKey, TopicConsumerValue}
import hydra.kafka.model.TopicConsumerOffset.{TopicConsumerOffsetKey, TopicConsumerOffsetValue}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model.{TopicConsumer, TopicConsumerOffset}
import hydra.kafka.util.ConsumerGroupsOffsetConsumer
import hydra.kafka.util.KafkaUtils.TopicDetails
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.avro.generic.GenericRecord
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{Assertion, BeforeAndAfterAll}
import retry.RetryPolicies._
import retry.syntax.all._
import retry.{RetryDetails, RetryPolicy}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try

class ConsumerGroupsAlgebraSpec extends AnyWordSpecLike with Matchers with ForAllTestContainer with BeforeAndAfterAll {

  override val container: KafkaContainer = KafkaContainer()
  container.start()

  implicit private val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect

  private implicit val policy: RetryPolicy[IO] = limitRetries[IO](5) |+| exponentialBackoff[IO](500.milliseconds)
  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  private implicit def noop[A]: (A, RetryDetails) => IO[Unit] = retry.noop[IO, A]

  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] =
    Slf4jLogger.getLogger[F]

  private implicit class RetryAndAssert[A](boolIO: IO[A]) {
    def retryIfFalse(check: A => Boolean): IO[Assertion] =
      boolIO.map(check).retryingM(identity, policy, noop).map(assert(_))
  }

  override def afterAll(): Unit = {
    container.stop()
    super.afterAll()
  }

  private val internalKafkaConsumerTopic = "__consumer_offsets"
  private val dvsConsumerTopic = Subject.createValidated("_hydra.consumer-groups").get
  private val dvsInternalKafkaOffsetsTopic = Subject.createValidated("_hydra.consumer-offsets-offsets").get
  private val consumerGroup = "consumerGroupName"

  (for {
    kafkaAdmin <- KafkaAdminAlgebra.live[IO](container.bootstrapServers)
    schemaRegistry <- SchemaRegistry.test[IO]
    kafkaClient <- KafkaClientAlgebra.live[IO](container.bootstrapServers, schemaRegistry)
    consumerGroupAlgebra <- ConsumerGroupsAlgebra.make(internalKafkaConsumerTopic, dvsConsumerTopic, dvsInternalKafkaOffsetsTopic, container.bootstrapServers, consumerGroup, consumerGroup, kafkaClient, kafkaAdmin, schemaRegistry)
    _ <- consumerGroupAlgebra.startConsumer
  } yield {
    runTests(consumerGroupAlgebra, schemaRegistry, kafkaClient, kafkaAdmin)
  }).unsafeRunSync()

  def runTests(
                cga: ConsumerGroupsAlgebra[IO],
                schemaRegistry: SchemaRegistry[IO],
                kafkaClient: KafkaClientAlgebra[IO],
                kafkaAdmin: KafkaAdminAlgebra[IO]): Unit = {

    Try(createDVSConsumerTopic(schemaRegistry, kafkaAdmin))
    Try(createDVSInternalKafkaOffsetsTopic(schemaRegistry, kafkaAdmin))

    val topicName = "useless_topic_123"
    val (keyGR, valueGR) = getGenericRecords(topicName, "key123", "value123")
    createTopic(topicName, keyGR, valueGR, schemaRegistry, kafkaAdmin)
    kafkaClient.publishMessage((keyGR, Some(valueGR), None), topicName).unsafeRunSync()
    val consumer1 = "randomConsumerGroup"
    kafkaClient.consumeMessages(topicName, consumer1, commitOffsets = true).take(1).compile.last.unsafeRunSync() shouldBe (keyGR, valueGR.some, None).some

    "ConsumerGroupAlgebraSpec" should {

      "consume randomConsumerGroup offsets into the cache" in {
        cga.getConsumersForTopic(topicName).retryIfFalse(_.consumers.exists(_.consumerGroupName == "randomConsumerGroup")).unsafeRunSync()
      }

      "consume two consumerGroups on useless_topic_123 topic into the cache" in {
        val consumer2 = "randomConsumerGroup2"
        kafkaClient.consumeMessages(topicName, consumer2, commitOffsets = true).take(1).compile.last.unsafeRunSync() shouldBe (keyGR, valueGR.some, None).some
        cga.getConsumersForTopic(topicName).retryIfFalse { c =>
          val consumers = List(consumer1, consumer2)
          c.consumers.length == 2 && c.consumers.map(_.consumerGroupName).forall(consumers.contains)
        }.unsafeRunSync()
      }

      "consume two topics of the same consumer group randomConsumerGroup" in {
        val topicName2 = "myNewTopix"
        val (keyGR2, valueGR2) = getGenericRecords(topicName2, "abc", "123")
        createTopic(topicName2, keyGR2, valueGR2, schemaRegistry, kafkaAdmin)
        kafkaClient.publishMessage((keyGR2, Some(valueGR2), None), topicName2).unsafeRunSync()
        kafkaClient.consumeMessages(topicName2, consumer1, commitOffsets = true).take(1).compile.last.unsafeRunSync() shouldBe (keyGR2, valueGR2.some, None).some
        val topics = List(topicName, topicName2)
        cga.getTopicsForConsumer(consumer1).retryIfFalse(_.topics.map(_.topicName).forall(topics.contains)).unsafeRunSync()
      }

      "confirm that the timestamps for the consumed topics vary from one to the other" in {
        cga.getAllConsumers.map(_.toSet.size).unsafeRunSync() shouldBe cga.getAllConsumers.map(_.size).unsafeRunSync()
      }

      "update the consumer offset, verify that the latest timestamp is updated" in {
        val topicName2 = "myNewTopix2.0"
        val (keyGR2, valueGR2) = getGenericRecords(topicName2, "abcdef", "123456")
        createTopic(topicName2, keyGR2, valueGR2, schemaRegistry, kafkaAdmin)
        kafkaClient.publishMessage((keyGR2, Some(valueGR2), None), topicName2).unsafeRunSync()

        val (keyGR3, valueGR3) = getGenericRecords(topicName2, "abcdef", "123456")
        kafkaClient.publishMessage((keyGR3, Some(valueGR3), None), topicName2).unsafeRunSync()

        kafkaClient.consumeMessages(topicName2, "randomConsumerGroup", commitOffsets = true).take(1).compile.last.unsafeRunSync() shouldBe (keyGR2, valueGR2.some, None).some
        cga.getConsumersForTopic(topicName2).map(_.consumers.headOption.map(_.lastCommit)).retryIfFalse(_.isDefined).unsafeRunSync()
        val firstTimestamp = cga.getConsumersForTopic(topicName2).map(_.consumers.headOption.map(_.lastCommit)).unsafeRunSync().get

        kafkaClient.consumeMessages(topicName2, "randomConsumerGroup", commitOffsets = true).take(1).compile.last.unsafeRunSync() shouldBe (keyGR3, valueGR3.some, None).some
        cga.getConsumersForTopic(topicName2).map(_.consumers.head.lastCommit).retryIfFalse(_.isAfter(firstTimestamp)).unsafeRunSync()
      }

      "test the getOffsets function" in {
        def testConsumerGroupsAlgebraGetOffsetsToSeekTo(
                                                          numberOfPartitionsForKafkaInternalTopic: Int,
                                                         latestPartitionOffset: PartitionOffsetMap,
                                                         dvsConsumerOffsetStream: fs2.Stream[IO, (Record, OffsetInfo)]
                                                       ) = {
          (for {
            consumerOffsetsCache <- Ref[IO].of((0 to numberOfPartitionsForKafkaInternalTopic).map((_, 0L)).toMap)
            hydraConsumerOffsetsOffsetsCache <- Ref[IO].of(latestPartitionOffset.filter(_._2 == 0))
            deferred <- Deferred[IO, PartitionOffsetMap]
            backgroundProcess <- Concurrent[IO].start {
              ConsumerGroupsOffsetConsumer.getOffsetsToSeekTo(
                consumerOffsetsCache = consumerOffsetsCache,
                deferred = deferred,
                dvsConsumerOffsetStream = dvsConsumerOffsetStream,
                hydraConsumerOffsetsOffsetsLatestOffsets = latestPartitionOffset,
                hydraConsumerOffsetsOffsetsCache = hydraConsumerOffsetsOffsetsCache
              )
            }
            _ <- deferred.get
            _ <- backgroundProcess.cancel
          } yield succeed).unsafeRunSync()
        }

        val c = createOV(Ref[IO].of((0,-1L)).unsafeRunSync()) _

        val latestPartitionMap = Map[Int, Long](0 -> 3, 1 -> 2, 2 -> 1)
        val dvsConsumerOffsetStream = fs2.Stream(c(false),c(false),c(false),c(true),c(false),c(true))
        testConsumerGroupsAlgebraGetOffsetsToSeekTo(3, latestPartitionMap, dvsConsumerOffsetStream)

        val latestPartitionMap2 = Map[Int, Long](0 -> 0, 1 -> 0, 2 -> 0)
        val dvsConsumerOffsetStream2 = fs2.Stream()
        testConsumerGroupsAlgebraGetOffsetsToSeekTo(3, latestPartitionMap2, dvsConsumerOffsetStream2)

        val d = createOV(Ref[IO].of((0,-1L)).unsafeRunSync()) _
        val latestPartitionMap3 = Map[Int, Long](0 -> 3, 1 -> 2, 2 -> 1, 3 -> 0)
        val dvsConsumerOffsetStream3 = fs2.Stream(d(false),d(false),d(false),d(true),d(false),d(true))
        testConsumerGroupsAlgebraGetOffsetsToSeekTo(4, latestPartitionMap3, dvsConsumerOffsetStream3)

        IO.race(IO.delay(testConsumerGroupsAlgebraGetOffsetsToSeekTo(3, latestPartitionMap, fs2.Stream.empty)), Timer[IO].sleep(1.seconds)).map {
          case Left(_) => fail("Should never have completed")
          case Right(_) => succeed
        }.unsafeRunSync()
      }
    }
  }

  private def createOV(partitionCounter: Ref[IO, OffsetInfo])(incrementPartition: Boolean): (Record, OffsetInfo) = {
    (partitionCounter.update { case (partition, offset) =>
      if (incrementPartition) {
        (partition + 1, 0)
      } else {
        (partition, offset + 1)
      }
    } *> partitionCounter.get.map { case (partition, offset) =>
      val key = TopicConsumerOffsetKey("topicName", partition)
      val value = TopicConsumerOffsetValue(scala.util.Random.nextLong())
      val (gKey, gValue) = TopicConsumerOffset.encode[IO](key, value).unsafeRunSync()
      ((gKey, gValue.some, None),(partition, offset))
    }).unsafeRunSync()
  }

  private def createTopic(subject: String, keyGR: GenericRecord, valueGR: GenericRecord, schemaRegistry: SchemaRegistry[IO], kafkaAdminAlgebra: KafkaAdminAlgebra[IO]): Unit = {
    (kafkaAdminAlgebra.createTopic(subject, TopicDetails(1, 1)) *>
    schemaRegistry.registerSchema(s"$subject-key", keyGR.getSchema) *>
    schemaRegistry.registerSchema(s"$subject-value", valueGR.getSchema)).unsafeRunSync()
  }

  /*
      Uses instance of TopicConsumerKey and Value to get schemas for TopicConsumer
      Returns those instances to be used as proof that the dvsConsumerTopic can publish and consume records
   */
  private def createDVSConsumerTopic(schemaRegistry: SchemaRegistry[IO], kafkaAdminAlgebra: KafkaAdminAlgebra[IO]): (GenericRecord, GenericRecord) = {
    val topic = dvsConsumerTopic.value
    TopicConsumer.encode[IO](TopicConsumerKey("t", "c"), TopicConsumerValue(Instant.now).some).flatMap { case (k, Some(v)) =>
      kafkaAdminAlgebra.createTopic(topic, TopicDetails(1, 1)).flatMap { _ =>
        schemaRegistry.registerSchema(topic, k.getSchema) *>
          schemaRegistry.registerSchema(topic, v.getSchema)
      } *>
      IO.pure((k, v))
    case _ => IO.pure(null, null)
    }.unsafeRunSync()
  }

  private def createDVSInternalKafkaOffsetsTopic(schemaRegistry: SchemaRegistry[IO], kafkaAdminAlgebra: KafkaAdminAlgebra[IO]): (GenericRecord, GenericRecord) = {
    val topic = dvsInternalKafkaOffsetsTopic.value
    TopicConsumerOffset.encode[IO](TopicConsumerOffsetKey("t", 0), TopicConsumerOffsetValue(0)).flatMap { case (k, v) =>
      kafkaAdminAlgebra.createTopic(topic, TopicDetails(1, 1)).flatMap { _ =>
        schemaRegistry.registerSchema(topic, k.getSchema) *>
          schemaRegistry.registerSchema(topic, v.getSchema)
      } *>
        IO.pure((k, v))
    }.unsafeRunSync()
  }

  private def getGenericRecords(subject: String, keyValue: String, value: String): (GenericRecord, GenericRecord) = {
    val (_, (_, keyRecord), valueRecord) = KafkaClientAlgebraSpec.topicAndKeyAndValue(subject, keyValue, value)
    (keyRecord, valueRecord)
  }

}