package hydra.kafka.algebras

import java.time.Instant
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, ContextShift, IO, Sync, Timer}
import cats.implicits._
import com.dimafeng.testcontainers.{ForAllTestContainer, KafkaContainer}
import hydra.avro.registry.SchemaRegistry
import hydra.common.config.KafkaConfigUtils.{KafkaClientSecurityConfig, SchemaRegistrySecurityConfig, kafkaSecurityEmptyConfig}
import hydra.kafka.algebras.ConsumerGroupsAlgebra.PartitionOffsetMap
import hydra.kafka.algebras.KafkaClientAlgebra.{OffsetInfo, Record}
import hydra.kafka.model.TopicConsumer.{TopicConsumerKey, TopicConsumerValue}
import hydra.kafka.model.TopicConsumerOffset.{TopicConsumerOffsetKey, TopicConsumerOffsetValue}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model.{TopicConsumer, TopicConsumerOffset, TopicMetadataV2, TopicMetadataV2Key}
import hydra.kafka.util.ConsumerGroupsOffsetConsumer
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.avro.generic.GenericRecord
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{Assertion, BeforeAndAfterAll}
import retry.RetryPolicies._
import retry.syntax.all._
import retry.{RetryDetails, RetryPolicy}
import vulcan.Codec
import vulcan.generic._

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
      boolIO.map(check).retryingOnFailures(identity, policy, noop).map(assert(_))
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
    kafkaAdmin <- KafkaAdminAlgebra.live[IO](container.bootstrapServers,  kafkaSecurityEmptyConfig)
    schemaRegistry <- SchemaRegistry.test[IO]
    kafkaClient <- KafkaClientAlgebra.live[IO](container.bootstrapServers, "https://schema-registry", schemaRegistry , kafkaSecurityEmptyConfig)
    consumerGroupAlgebra <- ConsumerGroupsAlgebra.make(internalKafkaConsumerTopic, dvsConsumerTopic, dvsInternalKafkaOffsetsTopic, container.bootstrapServers, consumerGroup, consumerGroup, kafkaClient, kafkaAdmin, schemaRegistry,  kafkaSecurityEmptyConfig)
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

      "sfgfg" in {
        (for {
          schemaRegistry <- SchemaRegistry.live[IO]("http://localhost:8081", 1,   SchemaRegistrySecurityConfig(None, None))
          kafkaClient <- KafkaClientAlgebra.live[IO]("kafka:29092", "https://schema-registry" ,  schemaRegistry, kafkaSecurityEmptyConfig)
          s <- TopicMetadataV2.encode[IO](TopicMetadataV2Key(Subject.createValidated("dvs.data-platform.v-1.oleksii10").get), None, None)
          _ <- kafkaClient.publishMessage(s, "_hydra.v2.metadata")
          _ <- kafkaClient.publishMessage(s, "_hydra.v2.metadata")
          _ <- kafkaClient.publishMessage(s, "_hydra.v2.metadata")
        } yield()).unsafeRunSync()
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
    (kafkaAdminAlgebra.createTopic(subject, TopicDetails(1, 1, 1)) *>
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
      kafkaAdminAlgebra.createTopic(topic, TopicDetails(1, 1, 1)).flatMap { _ =>
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
      kafkaAdminAlgebra.createTopic(topic, TopicDetails(1, 1, 1)).flatMap { _ =>
        schemaRegistry.registerSchema(topic, k.getSchema) *>
          schemaRegistry.registerSchema(topic, v.getSchema)
      } *>
        IO.pure((k, v))
    }.unsafeRunSync()
  }

  private def getGenericRecords(subject: String, keyValue: String, value: String): (GenericRecord, GenericRecord) = {
    val (_, (_, keyRecord), valueRecord) = ConsumerGroupsAlgebraSpec.topicAndKeyAndValue(subject, keyValue, value)
    (keyRecord, valueRecord)
  }

}

object ConsumerGroupsAlgebraSpec {
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

  def topicAndKeyAndValue(topic: String, key: String, value: String): (String, (Option[String], GenericRecord), GenericRecord) = {
    (topic,
      (Some(key), SimpleCaseClassKey.codec.encode(SimpleCaseClassKey(key)).map(_.asInstanceOf[GenericRecord]).toOption.get),
      SimpleCaseClassValue.codec.encode(SimpleCaseClassValue(value)).map(_.asInstanceOf[GenericRecord]).toOption.get)
  }

}