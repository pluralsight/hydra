package hydra.kafka.model

import java.time.Instant

import cats.effect.IO
import hydra.kafka.model.TopicConsumer.{TopicConsumerKey, TopicConsumerValue}
import org.apache.avro.generic.GenericRecordBuilder
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class TopicConsumerSpec extends AnyFlatSpecLike with Matchers {

  val commitDate = Instant.now

  it must "encode TopicConsumer key and value" in {
    val topic = "TopicName"
    val consumerGroup = "ConsumerGroup"
    val key = TopicConsumerKey(topic, consumerGroup)
    val value = TopicConsumerValue(commitDate)

    val (encodedKey, encodedValue) =
      TopicConsumer.encode[IO](key, Some(value)).unsafeRunSync()

    val genericKey = new GenericRecordBuilder(TopicConsumerKey.codec.schema.toOption.get)
      .set("topicName", key.topicName)
      .set("consumerGroupName", key.consumerGroupName)
      .build()

    val genericValue = new GenericRecordBuilder(TopicConsumerValue.codec.schema.toOption.get)
      .set("lastCommit", value.lastCommit.toEpochMilli)
      .build()

    val (decodedKey, decodedValue) =
      TopicConsumer.decode[IO](genericKey, Some(genericValue)).unsafeRunSync()

    encodedKey shouldBe genericKey
    encodedValue shouldBe Some(genericValue)
    decodedKey shouldBe key
    decodedValue.get shouldBe value
  }
}
