package hydra.kafka.algebras

import java.time.Instant

import cats.effect.{Concurrent, ContextShift, IO, Sync, Timer}
import cats.implicits._
import com.dimafeng.testcontainers.{ForAllTestContainer, KafkaContainer}
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.model.TopicConsumer
import hydra.kafka.model.TopicConsumer.{TopicConsumerKey, TopicConsumerValue}
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
  private val dvsConsumerTopic = "dvs_internal_consumers1"
  private val consumerGroup = "consumerGroupName"

  (for {
    kafkaAdmin <- KafkaAdminAlgebra.test[IO]//(container.bootstrapServers)
    schemaRegistry <- SchemaRegistry.test[IO]
    kafkaClient <- KafkaClientAlgebra.live[IO](container.bootstrapServers, schemaRegistry)
    consumerGroupAlgebra <- ConsumerGroupsAlgebra.make(internalKafkaConsumerTopic, dvsConsumerTopic, container.bootstrapServers, consumerGroup, consumerGroup, kafkaClient, schemaRegistry)
  } yield {
    runTests(consumerGroupAlgebra, schemaRegistry, kafkaClient, kafkaAdmin)
  }).unsafeRunSync()

  def runTests(
                cga: ConsumerGroupsAlgebra[IO],
                schemaRegistry: SchemaRegistry[IO],
                kafkaClient: KafkaClientAlgebra[IO],
                kafkaAdmin: KafkaAdminAlgebra[IO]): Unit = {

    createDVSConsumerTopic(schemaRegistry, kafkaAdmin)

    val topicName = "dvs_internal_test123"
    val (keyGR, valueGR) = getGenericRecords(topicName, "key123", "value123")
    createTopic(topicName, keyGR, valueGR, schemaRegistry, kafkaAdmin)
    kafkaClient.publishMessage((keyGR, Some(valueGR)), topicName).unsafeRunSync()
    kafkaClient.consumeMessages(topicName, "randomConsumerGroup", commitOffsets = true).take(1).compile.last.unsafeRunSync() shouldBe (keyGR, valueGR.some).some

    "ConsumerGroupAlgebraSpec" should {

      "consume randomConsumerGroup offsets into the cache" in {
        cga.getConsumersForTopic(topicName).retryIfFalse(_.consumers.exists(_.consumerGroupName == "randomConsumerGroup")).unsafeRunSync()
      }

      "consume two consumerGroups on dvs_internal_test123 topic into the cache" in {
        kafkaClient.consumeMessages(topicName, "randomConsumerGroup2", commitOffsets = true).take(1).compile.last.unsafeRunSync() shouldBe (keyGR, valueGR.some).some
        cga.getConsumersForTopic(topicName).retryIfFalse(_.consumers.length == 2).unsafeRunSync()
        cga.getConsumersForTopic(topicName).unsafeRunSync()
      }

      "consume two topics of the same consumer group randomConsumerGroup" in {
        val topicName2 = "myNewTopix"
        val (keyGR2, valueGR2) = getGenericRecords(topicName2, "abc", "123")
        createTopic(topicName2, keyGR2, valueGR2, schemaRegistry, kafkaAdmin)
        kafkaClient.publishMessage((keyGR2, Some(valueGR2)), topicName2).unsafeRunSync()
        kafkaClient.consumeMessages(topicName2, "randomConsumerGroup", commitOffsets = true).take(1).compile.last.unsafeRunSync() shouldBe (keyGR2, valueGR2.some).some

        cga.getTopicsForConsumer("randomConsumerGroup").retryIfFalse(_.topics.map(_.topicName).forall(List(topicName, topicName2).contains)).unsafeRunSync()
      }

      "confirm that the timestamps for the consumed topics vary from one to the other" in {
        cga.getAllConsumers.map(_.values.toSet.size).unsafeRunSync() shouldBe cga.getAllConsumers.map(_.values.size).unsafeRunSync()
      }

      "update the consumer offset, verify that the latest timestamp is updated" in {
        val topicName2 = "myNewTopix2.0"
        val (keyGR2, valueGR2) = getGenericRecords(topicName2, "abcdef", "123456")
        createTopic(topicName2, keyGR2, valueGR2, schemaRegistry, kafkaAdmin)
        kafkaClient.publishMessage((keyGR2, Some(valueGR2)), topicName2).unsafeRunSync()

        val (keyGR3, valueGR3) = getGenericRecords(topicName2, "abcdef", "123456")
        kafkaClient.publishMessage((keyGR3, Some(valueGR3)), topicName2).unsafeRunSync()

        kafkaClient.consumeMessages(topicName2, "randomConsumerGroup", commitOffsets = true).take(1).compile.last.unsafeRunSync() shouldBe (keyGR2, valueGR2.some).some
        cga.getConsumersForTopic(topicName2).map(_.consumers.headOption.map(_.lastCommit)).retryIfFalse(_.isDefined).unsafeRunSync()
        val firstTimestamp = cga.getConsumersForTopic(topicName2).map(_.consumers.headOption.map(_.lastCommit)).unsafeRunSync().get

        kafkaClient.consumeMessages(topicName2, "randomConsumerGroup", commitOffsets = true).take(1).compile.last.unsafeRunSync() shouldBe (keyGR3, valueGR3.some).some

        cga.getConsumersForTopic(topicName2).map(_.consumers.head.lastCommit).retryIfFalse(_.isAfter(firstTimestamp)).unsafeRunSync()
      }
    }
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
    val topic = dvsConsumerTopic
    TopicConsumer.encode[IO](TopicConsumerKey("t", "c"), TopicConsumerValue(Instant.now).some).flatMap { case (k, Some(v)) =>
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