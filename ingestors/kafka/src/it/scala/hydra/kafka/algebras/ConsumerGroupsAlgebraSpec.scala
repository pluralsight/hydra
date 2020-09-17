package hydra.kafka.algebras

import java.time.Instant

import cats.effect.{Concurrent, ContextShift, IO, Sync, Timer}
import cats.implicits._
import com.dimafeng.testcontainers.{ForAllTestContainer, KafkaContainer}
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.KafkaClientAlgebraSpec.SimpleCaseClassValue
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

  def runTests(cga: ConsumerGroupsAlgebra[IO], schemaRegistry: SchemaRegistry[IO], kafkaClient: KafkaClientAlgebra[IO], kafkaAdmin: KafkaAdminAlgebra[IO]): Unit = {
    val (dvsTopicKey, dvsTopicValue) = createDVSConsumerTopic(schemaRegistry, kafkaAdmin)
    kafkaClient.publishMessage((dvsTopicKey, dvsTopicValue.some), dvsConsumerTopic).unsafeRunSync()
    kafkaClient.consumeMessages(dvsConsumerTopic, "tempConsumerGroup").take(1).compile.last.unsafeRunSync() shouldBe (dvsTopicKey, dvsTopicValue.some).some


    val topicName = "dvs_internal_test123"
    val (keyGR, valueGR) = getGenericRecords(topicName, "key123", "value123")
    createTopic(topicName, keyGR, valueGR, schemaRegistry, kafkaAdmin)
    kafkaClient.publishMessage((keyGR, Some(valueGR)), topicName).unsafeRunSync()
    kafkaClient.consumeMessages(topicName, "randomConsumerGroup").take(1).compile.last.unsafeRunSync() shouldBe (keyGR, valueGR.some).some


    cga.internalStream(container.bootstrapServers, "DNM").take(7).map(println).compile.drain.unsafeRunSync()

    println("WE\'VE PASSED THE INTERNAL STREAM!!!")

    kafkaClient.consumeMessages(dvsConsumerTopic, "doesNotMatter").take(2).map { case (g, _) =>
      println(g)
    }.compile.drain.unsafeRunSync()

    "ConsumerGroupAlgebraSpec" should {

      "consume offsets into the internal topic" in {
        cga.getConsumersForTopic(topicName).retryIfFalse(_.consumers.nonEmpty).unsafeRunSync()
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

// 1) __consumer_offsets topic is not actually committing offset info (Maybe the client's offset committing is wrong!)
// 2) __consumer_offsets is working, but publishing to the dvs_internal_consumers topic is failing
// 3) dvs_internal_consumers topic is not being consumed into the cache
