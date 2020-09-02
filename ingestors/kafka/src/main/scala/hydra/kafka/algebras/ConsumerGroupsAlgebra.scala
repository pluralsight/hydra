package hydra.kafka.algebras

import java.nio.ByteBuffer
import java.time.Instant

import cats.{Monad, MonadError}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import fs2.Chunk.Bytes
import fs2.concurrent.Queue
import fs2.kafka._
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.ConsumerGroupsAlgebra.{Consumer, ConsumerTopics, Topic, TopicConsumerKey, TopicConsumerValue, TopicConsumers}
import hydra.kafka.algebras.KafkaClientAlgebra.PublishError.RecordTooLarge
import hydra.kafka.algebras.KafkaClientAlgebra.{GenericRecordFormat, RecordFormat, StringFormat, TopicName}
import io.chrisdavenport.log4cats.Logger
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import vulcan.generic._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import kafka.common.OffsetAndMetadata
import kafka.coordinator.group.{BaseKey, GroupMetadataKey, GroupMetadataManager, OffsetKey}
import vulcan.Codec

import scala.util.Try

trait ConsumerGroupsAlgebra[F[_]] {
}

object ConsumerGroupsAlgebra {
  object TopicConsumerKey {
    implicit val codec: Codec[TopicConsumerKey] = Codec.derive[TopicConsumerKey]
  }
  final case class TopicConsumerKey(topicName: String, consumerGroupName: String)

  object TopicConsumerValue {
    implicit val codec: Codec[TopicConsumerValue] = Codec.derive[TopicConsumerValue]
  }
  final case class TopicConsumerValue(lastCommit: Instant)

  final case class TopicConsumers(topicName: String, consumers: List[Consumer])
  final case class Consumer(consumerGroupName: String, lastCommit: Instant)

  final case class ConsumerTopics(consumerGroupName: String, topics: List[Topic])
  final case class Topic(topicName: String, lastCommit: Instant)


  def make[F[_]: ContextShift: ConcurrentEffect: Timer](kafkaInternalTopic: String, dvsConsumerTopic: String, bootstrapServers: String, consumerGroup: String, kafkaClientAlgebra: KafkaClientAlgebra[F], schemaRegistryAlgebra: SchemaRegistry[F]): F[ConsumerGroupsAlgebra[F]] = {
    val b = for {
      cf <- Ref[F].of(ConsumerGroupsStorageFacade.empty)
      schemaRegistryClient <- schemaRegistryAlgebra.getSchemaRegistryClient
      _ <- Concurrent[F].start(consumerOffsetsToInternalOffsets(kafkaInternalTopic, dvsConsumerTopic, bootstrapServers, consumerGroup, schemaRegistryClient))
    } yield ()

    Sync[F].delay(new ConsumerGroupsAlgebra[F] {
    })
  }

  private def consumerOffsetsToInternalOffsets[F[_]: ConcurrentEffect: ContextShift: Timer](sourceTopic: String, destinationTopic: String, bootstrapServers: String, consumerGroupName: String, s: SchemaRegistryClient) = {
    val settings: ConsumerSettings[F, Option[BaseKey], Option[OffsetAndMetadata]] = ConsumerSettings(
      getConsumerGroupDeserializer[F, BaseKey](GroupMetadataManager.readMessageKey),
      getConsumerGroupDeserializer[F, OffsetAndMetadata](GroupMetadataManager.readOffsetMessageValue)
    )
      .withAutoOffsetReset(AutoOffsetReset.Latest)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(consumerGroupName)

    val producerSettings = ProducerSettings[F, Array[Byte], Array[Byte]]
      .withBootstrapServers(bootstrapServers)
      .withAcks(Acks.All)

    consumerStream(settings)
      .evalTap(_.subscribeTo(sourceTopic))
      .flatMap(_.stream)
      .evalTap(_.offset.commit)
      .flatMap { cr =>
        (cr.record.key, cr.record.value) match {
          case (Some(OffsetKey(_, k)), offsetMaybe) =>
            val topic = k.topicPartition.topic()
            val consumerGroup = k.group
            val key: GenericRecord = new GenericRecordBuilder(TopicConsumerKey.codec.schema.toOption.get).set("topicName",topic).set("consumerGroupName",consumerGroup).build()
            val value: Option[GenericRecord] = offsetMaybe.map(o => Instant.ofEpochMilli(o.commitTimestamp)).map(commit => new GenericRecordBuilder(TopicConsumerValue.codec.schema.toOption.get).set("lastCommit",commit).build())

            fs2.Stream.eval(for {
              k <- getSerializer[F, GenericRecord](s)(isKey = true).serialize(destinationTopic, Headers.empty, key)
              v <- getSerializer[F, Option[GenericRecord]](s)(isKey = false).serialize(destinationTopic, Headers.empty, value)
            } yield ProducerRecord(destinationTopic, k, v)).map(ProducerRecords.one)
          case _ =>
            fs2.Stream.empty
        }
      }
      .through(produce(producerSettings))
      .compile.drain
  }

  private def getSerializer[F[_]: Sync, A](schemaRegistryClient: SchemaRegistryClient)(isKey: Boolean): Serializer[F, A] =
    Serializer.delegate[F, A] {
      val serializer = {
        val se = new KafkaAvroSerializer(schemaRegistryClient)
        se.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "").asJava, isKey)
        se
      }
      (topic: TopicName, data: A) => serializer.serialize(topic, data)
    }.suspend

  private def getConsumerGroupDeserializer[F[_]: Sync, A](byteBufferToA: ByteBuffer => A): Deserializer[F, Option[A]] =
    Deserializer.delegate[F, Option[A]] {
      (_: TopicName, data: Array[Byte]) => {
        Try(byteBufferToA(Bytes(data).toByteBuffer)).toOption
      }
    }.suspend
}

private case class ConsumerGroupsStorageFacade(consumerMap: Map[TopicConsumerKey, TopicConsumerValue]) {
  def addConsumerGroup(key: TopicConsumerKey, value: TopicConsumerValue): ConsumerGroupsStorageFacade =
    this.copy(this.consumerMap + (key -> value))
  def removeConsumerGroup(key: TopicConsumerKey): ConsumerGroupsStorageFacade =
    this.copy(this.consumerMap - key)
  def getConsumersForTopicName(topicName: String): TopicConsumers = {
    val consumerGroups = consumerMap.filterKeys(_.topicName == topicName).map(p => Consumer(p._1.consumerGroupName, p._2.lastCommit)).toList
    TopicConsumers(topicName, consumerGroups)
  }
  def getTopicsForConsumerGroupName(consumerGroupName: String): ConsumerTopics = {
    val topics = consumerMap.filterKeys(_.consumerGroupName == consumerGroupName).map(p => Topic(p._1.topicName, p._2.lastCommit)).toList
    ConsumerTopics(consumerGroupName, topics)
  }
}

private object ConsumerGroupsStorageFacade {
  def empty: ConsumerGroupsStorageFacade = ConsumerGroupsStorageFacade(Map.empty)
}

