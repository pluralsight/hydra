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
import cats.syntax.all._
import hydra.kafka.algebras.KafkaClientAlgebra.PublishError.{RecordTooLarge, TopicNotFoundInMetadata}
import hydra.kafka.algebras.KafkaClientAlgebra.{OffsetInfoNotRetrievableInTest, PublishResponse}
import hydra.kafka.util.KafkaUtils.TopicDetails
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext

class KafkaClientAlgebraSpec
  extends AnyWordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with EmbeddedKafka {

  import KafkaClientAlgebraSpec._

  private val port = 8092

  implicit private val kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = port, zooKeeperPort = 3182, Map("auto.create.topics.enable" -> "false"))

  implicit private val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect
  implicit private val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
    createTopics.unsafeRunSync()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EmbeddedKafka.stop()
  }

  private def createTopics: IO[Unit] = {
    val topicsToCreate: List[String] = List("topic1", "topic2", "stringTopic1", "stringTopic2", "nullTopic1")
    KafkaAdminAlgebra.live[IO](s"localhost:$port")
      .flatMap(adminClient => topicsToCreate.traverse(adminClient.createTopic(_, TopicDetails(1, 1)))).void
  }

  private implicit val catsLogger: SelfAwareStructuredLogger[IO] =
    Slf4jLogger.getLogger[IO]

  (for {
    schemaRegistryAlgebra1 <- SchemaRegistry.test[IO]
    schemaRegistryAlgebra2 <- SchemaRegistry.test[IO]
    live <- KafkaClientAlgebra.live[IO](s"localhost:$port", schemaRegistryAlgebra1, recordSizeLimit = None)
    test <- KafkaClientAlgebra.test[IO](schemaRegistryAlgebra2)
  } yield {
    runTest(schemaRegistryAlgebra1, live)
    runTest(schemaRegistryAlgebra2, test, isTest = true)
  }).unsafeRunSync()

  private def runTest(schemaRegistry: SchemaRegistry[IO], kafkaClient: KafkaClientAlgebra[IO], isTest: Boolean = false): Unit = {
    (if (isTest) "KafkaClient#test" else "KafkaClient#live") must {
      avroTests(schemaRegistry, kafkaClient, !isTest)
      stringKeyTests(schemaRegistry, kafkaClient, !isTest)
      nullKeyTests(schemaRegistry, kafkaClient, !isTest)
      if (!isTest) {
        topicNotExistsTests(schemaRegistry, kafkaClient)
      }
    }
  }

  private def topicNotExistsTests(schemaRegistry: SchemaRegistry[IO], kafkaClient: KafkaClientAlgebra[IO]): Unit = {
    "return an error when publishing to a topic that does not exist" in {
      val (topic, (_, key), value) = topicAndKeyAndValue("doesnotexist","key1","value1")
      (schemaRegistry.registerSchema(subject = s"$topic-key", key.getSchema) *>
        schemaRegistry.registerSchema(subject = s"$topic-value", value.getSchema) *>
        kafkaClient.publishMessage((key, Some(value), None), topic)).unsafeRunSync().leftMap(_.getMessage) shouldBe
        Left("Topic doesnotexist was not found in metadata after 4500 ms.")
    }
  }

  private def avroTests(schemaRegistry: SchemaRegistry[IO], kafkaClient: KafkaClientAlgebra[IO], shouldCommitOffsets: Boolean): Unit = {
    "publish avro message to kafka" in {
      val (topic, (_, key), value) = topicAndKeyAndValue("topic1","key1","value1")
      (schemaRegistry.registerSchema(subject = s"$topic-key", key.getSchema) *>
        schemaRegistry.registerSchema(subject = s"$topic-value", value.getSchema) *>
        kafkaClient.publishMessage((key, Some(value), None), topic).map{ r =>
          r shouldBe PublishResponse(0, 0).asRight}).unsafeRunSync()
    }

    val (topic, (_, key), value) = topicAndKeyAndValue("topic1","key1","value1")
    "consume avro message from kafka" in {
      val records = kafkaClient.consumeMessages(topic,"newConsumerGroup", shouldCommitOffsets).take(1).compile.toList.unsafeRunSync()
      records should have length 1
      records.head shouldBe (key, Some(value), None)
    }

    "consume avro message with partition info from kafka" in {
      if (shouldCommitOffsets) {
        val records = kafkaClient.consumeMessagesWithOffsetInfo(topic,"partitionInfoConsumer", shouldCommitOffsets).take(1).compile.toList.unsafeRunSync()
        records should have length 1
        records.head shouldBe ((key, Some(value), None),(0,0))
      } else {
        a [OffsetInfoNotRetrievableInTest] should be thrownBy {
          kafkaClient.consumeMessagesWithOffsetInfo(topic,"partitionInfoConsumer", shouldCommitOffsets).take(1).compile.toList.unsafeRunSync()
        }
      }
    }

    val (_, (_, key2), value2) = topicAndKeyAndValue("topic1","key2","value2")
    "publish avro record to existing topic and consume only that value in existing consumer group" in {
      kafkaClient.publishMessage((key2, Some(value2), None), topic).unsafeRunSync()
      val records = kafkaClient.consumeMessages(topic, "newConsumerGroup6", shouldCommitOffsets).take(2).compile.toList.unsafeRunSync()
      records should contain allOf((key2, Some(value2), None), (key, Some(value), None))
    }

    val (_, (_, key3), value3) = topicAndKeyAndValue("topic1","key3","value3")
    "continue consuming avro messages from the same stream" in {
      val takeVal = if (shouldCommitOffsets) 1 else 3
      val stream = kafkaClient.consumeMessages(topic, "doesNotReallyMatter", shouldCommitOffsets)
      stream.take(2).compile.toList.unsafeRunSync() should contain allOf((key2, Some(value2), None), (key, Some(value), None))
      kafkaClient.publishMessage((key3, Some(value3), None), topic).unsafeRunSync()
      stream.take(takeVal).compile.toList.unsafeRunSync().last shouldBe (key3, Some(value3), None)
    }

    "return correct offsets from publish" in {
      kafkaClient.publishMessage((key2, Some(value2), None), topic).unsafeRunSync() shouldBe PublishResponse(0, 3).asRight
      kafkaClient.publishMessage((key2, Some(value2), None), topic).unsafeRunSync() shouldBe PublishResponse(0, 4).asRight
      kafkaClient.publishMessage((key2, Some(value2), None), topic).unsafeRunSync() shouldBe PublishResponse(0, 5).asRight
      kafkaClient.publishMessage((key2, Some(value2), None), topic).unsafeRunSync() shouldBe PublishResponse(0, 6).asRight
    }

    val (topic2, (_, key4), value4) = topicAndKeyAndValue("topic2","key4","value4")
    val (_, (_, key5), value5) = topicAndKeyAndValue("topic2","key5","value5")
    "consume avro messages from two different topics" in {
      (schemaRegistry.registerSchema(subject = s"$topic2-key", key4.getSchema) *>
        schemaRegistry.registerSchema(subject = s"$topic2-value", value4.getSchema) *>
        kafkaClient.publishMessage((key4, Some(value4), None), topic2) *>
        kafkaClient.publishMessage((key5, Some(value5), None), topic2)).unsafeRunSync()
      val topicOneStream = kafkaClient.consumeMessages(topic, "doesNotReallyMatter2", shouldCommitOffsets)
      val topicTwoStream = kafkaClient.consumeMessages(topic2, "doesNotReallyMatter2", shouldCommitOffsets)

      topicOneStream.take(3).compile.toList.unsafeRunSync() should contain allOf ((key3, Some(value3), None),
                                                                                  (key2, Some(value2), None),
                                                                                  (key, Some(value), None))
      topicTwoStream.take(2).compile.toList.unsafeRunSync() should contain allOf ((key4, Some(value4), None),
                                                                                  (key5, Some(value5), None))
    }

    "publish avro record to existing topic and be rejected by size limit" in {
      val result = kafkaClient.withProducerRecordSizeLimit(21)
        .flatMap(_.publishMessage((key2, Some(value2), None), topic)).attempt.unsafeRunSync()
      result shouldBe RecordTooLarge(22, 21).asLeft
    }

    "publish avro record to existing topic and not be rejected by size limit equal to size" in {
      val result = kafkaClient.withProducerRecordSizeLimit(22)
        .flatMap(_.publishMessage((key2, Some(value2), None), topic)).attempt.unsafeRunSync()
      result shouldBe a[Right[_, _]]
    }

    "publish avro record to existing topic and not be rejected by size limit larger than size" in {
      val result = kafkaClient.withProducerRecordSizeLimit(23)
        .flatMap(_.publishMessage((key2, Some(value2), None), topic)).attempt.unsafeRunSync()
      result shouldBe a[Right[_, _]]
    }
  }

  private def stringKeyTests(schemaRegistry: SchemaRegistry[IO], kafkaClient: KafkaClientAlgebra[IO], shouldCommitOffsets: Boolean): Unit = {
    "publish string key message to kafka" in {
      val (topic, (keyString, _), value) = topicAndKeyAndValue("stringTopic1","key1","value1")
      (schemaRegistry.registerSchema(subject = s"$topic-value", value.getSchema) *>
        kafkaClient.publishStringKeyMessage((keyString, Some(value), None), topic).map{ r =>
          assert(r.isRight)}).unsafeRunSync()
    }

    val (topic, (keyString, _), value) = topicAndKeyAndValue("stringTopic1","key1","value1")
    "consume string key message from kafka" in {
      val records = kafkaClient.consumeStringKeyMessages(topic,"newConsumerGroup2", shouldCommitOffsets).take(1).compile.toList.unsafeRunSync()
      records should have length 1
      records.head shouldBe (keyString, Some(value), None)
    }

    val (_, (keyString2, _), value2) = topicAndKeyAndValue("stringTopic1","key2","value2")
    "publish string key record to existing topic and consume only that value in existing consumer group" in {
      kafkaClient.publishStringKeyMessage((keyString2, Some(value2), None), topic).unsafeRunSync()
      val records = kafkaClient.consumeStringKeyMessages(topic, "newConsumerGroup6", shouldCommitOffsets).take(2).compile.toList.unsafeRunSync()
      records should contain allOf((keyString2, Some(value2), None), (keyString, Some(value), None))
    }

    val (_, (keyString3, _), value3) = topicAndKeyAndValue("stringTopic1","key3","value3")
    "continue consuming string key messages from the same stream" in {
      val takeVal = if (shouldCommitOffsets) 1 else 3
      val stream = kafkaClient.consumeStringKeyMessages(topic, "doesNotReallyMatter3", shouldCommitOffsets)
      stream.take(2).compile.toList.unsafeRunSync() should contain allOf((keyString2, Some(value2), None), (keyString, Some(value), None))
      kafkaClient.publishStringKeyMessage((keyString3, Some(value3), None), topic).unsafeRunSync()
      stream.take(takeVal).compile.toList.unsafeRunSync().last shouldBe (keyString3, Some(value3), None)
    }

    val (topic2, (keyString4, _), value4) = topicAndKeyAndValue("stringTopic2","key4","value4")
    val (_, (keyString5, _), value5) = topicAndKeyAndValue("stringTopic2","key5","value5")
    "consume string key messages from two different topics" in {
      (schemaRegistry.registerSchema(subject = s"$topic2-value", value4.getSchema) *>
        kafkaClient.publishStringKeyMessage((keyString4, Some(value4), None), topic2) *>
        kafkaClient.publishStringKeyMessage((keyString5, Some(value5), None), topic2)).unsafeRunSync()
      val topicOneStream = kafkaClient.consumeStringKeyMessages(topic, "doesNotReallyMatter4", shouldCommitOffsets)
      val topicTwoStream = kafkaClient.consumeStringKeyMessages(topic2, "doesNotReallyMatter4", shouldCommitOffsets)

      topicOneStream.take(3).compile.toList.unsafeRunSync() should contain allOf ((keyString3, Some(value3), None),
                                                                                  (keyString2, Some(value2), None),
                                                                                  (keyString,  Some(value),  None))
      topicTwoStream.take(2).compile.toList.unsafeRunSync() should contain allOf ((keyString4, Some(value4), None),
                                                                                  (keyString5, Some(value5), None))
    }
  }

  private def nullKeyTests(schemaRegistry: SchemaRegistry[IO], kafkaClient: KafkaClientAlgebra[IO], shouldCommitOffsets: Boolean): Unit = {
    "publish null key message to kafka" in {
      val (topic, _, value) = topicAndKeyAndValue("nullTopic1","key1","value1")
      (schemaRegistry.registerSchema(subject = s"$topic-value", value.getSchema) *>
        kafkaClient.publishStringKeyMessage((None, Some(value), None), topic).map{ r =>
          assert(r.isRight)}).unsafeRunSync()
    }

    val (topic, _, value) = topicAndKeyAndValue("nullTopic1","key1","value1")
    "consume null key message from kafka" in {
      val records = kafkaClient.consumeStringKeyMessages(topic,"newConsumerGroup3", shouldCommitOffsets).take(1).compile.toList.unsafeRunSync()
      records should have length 1
      records.head shouldBe (None, Some(value), None)
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

  def topicAndKeyAndValue(topic: String, key: String, value: String): (String, (Option[String], GenericRecord), GenericRecord) = {
    (topic,
      (Some(key), SimpleCaseClassKey.codec.encode(SimpleCaseClassKey(key)).map(_.asInstanceOf[GenericRecord]).toOption.get),
      SimpleCaseClassValue.codec.encode(SimpleCaseClassValue(value)).map(_.asInstanceOf[GenericRecord]).toOption.get)
  }
}
