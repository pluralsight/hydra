package hydra.kafka.algebras

import java.nio.ByteBuffer
import java.time.Instant

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import fs2.Chunk.Bytes
import fs2.kafka._
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.ConsumerGroupsAlgebra.{Consumer, ConsumerTopics, Topic, TopicConsumers}
import hydra.kafka.model.TopicConsumer
import hydra.kafka.model.TopicConsumer.{TopicConsumerKey, TopicConsumerValue}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import kafka.common.OffsetAndMetadata
import kafka.coordinator.group.{BaseKey, GroupMetadataManager, OffsetKey}
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._
import scala.util.Try

trait ConsumerGroupsAlgebra[F[_]] {
  def getConsumersForTopic(topicName: String): F[TopicConsumers]
  def getTopicsForConsumer(consumerGroupName: String): F[ConsumerTopics]
}

object ConsumerGroupsAlgebra {

  final case class TopicConsumers(topicName: String, consumers: List[Consumer])
  final case class Consumer(consumerGroupName: String, lastCommit: Instant)

  final case class ConsumerTopics(consumerGroupName: String, topics: List[Topic])
  final case class Topic(topicName: String, lastCommit: Instant)


  def make[F[_]: ContextShift: ConcurrentEffect: Timer](kafkaInternalTopic: String, dvsConsumerTopic: String, bootstrapServers: String, uniquePerNodeConsumerGroup: String, commonConsumerGroup: String, kafkaClientAlgebra: KafkaClientAlgebra[F], schemaRegistryAlgebra: SchemaRegistry[F]): F[ConsumerGroupsAlgebra[F]] = {
    val dvsConsumerStream = kafkaClientAlgebra.consumeMessages(dvsConsumerTopic, uniquePerNodeConsumerGroup)
    for {
      cf <- Ref[F].of(ConsumerGroupsStorageFacade.empty)
      schemaRegistryClient <- schemaRegistryAlgebra.getSchemaRegistryClient
      _ <- Concurrent[F].start(consumerOffsetsToInternalOffsets(kafkaInternalTopic, dvsConsumerTopic, bootstrapServers, commonConsumerGroup, schemaRegistryClient))
      _ <- Concurrent[F].start(dvsConsumerStream.flatMap { case (key, value) =>
        fs2.Stream.eval(TopicConsumer.decode[F](key, value).flatMap { case (topicKey, topicValue) =>
          topicValue match {
            case Some(tV) =>
              cf.update(_.addConsumerGroup(topicKey, tV))
            case None =>
              cf.update(_.removeConsumerGroup(topicKey))
          }
        })
      }.compile.drain)
    } yield new ConsumerGroupsAlgebra[F] {
      override def getConsumersForTopic(topicName: String): F[TopicConsumers] =
        cf.get.map(_.getConsumersForTopicName(topicName))

      override def getTopicsForConsumer(consumerGroupName: String): F[ConsumerTopics] =
        cf.get.map(_.getTopicsForConsumerGroupName(consumerGroupName))
    }
  }

  // TODO Use the MetadataAlgebra Consumer group for summarizedConsumerGroup Topic
  private def consumerOffsetsToInternalOffsets[F[_]: ConcurrentEffect: ContextShift: Timer](sourceTopic: String, destinationTopic: String, bootstrapServers: String, consumerGroupName: String, s: SchemaRegistryClient) = {
    val settings: ConsumerSettings[F, Option[BaseKey], Option[OffsetAndMetadata]] = ConsumerSettings(
      getConsumerGroupDeserializer[F, BaseKey](GroupMetadataManager.readMessageKey),
      getConsumerGroupDeserializer[F, OffsetAndMetadata](GroupMetadataManager.readOffsetMessageValue)
    )
      // TODO Potentially not the behavior desired (Earliest and commit offsets)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(consumerGroupName)

    val producerSettings = ProducerSettings[F, Array[Byte], Array[Byte]]
      .withBootstrapServers(bootstrapServers)
      .withAcks(Acks.All)

    consumerStream(settings)
      .evalTap(_.subscribeTo(sourceTopic))
      .flatMap(_.stream)
      // TODO Infinite Loop created when committing to the __consumer_offsets about the __consumer_offsets
//      .evalTap(_.offset.commit)
      .flatMap { cr =>
        (cr.record.key, cr.record.value) match {
          case (Some(OffsetKey(_, k)), offsetMaybe) =>
            val topic = k.topicPartition.topic()
            val consumerGroupMaybe = Option(k.group)
            val consumerKeyMaybe = consumerGroupMaybe.map(TopicConsumerKey(topic, _))
            val consumerValue = offsetMaybe.map(o => Instant.ofEpochMilli(o.commitTimestamp)).map(TopicConsumerValue.apply)

            consumerKeyMaybe match {
              case Some(consumerKey) =>
                fs2.Stream.eval(for {
                  topicConsumer <- TopicConsumer.encode[F](consumerKey, consumerValue)
                  (key, value) = topicConsumer
                  k <- getSerializer[F, GenericRecord](s)(isKey = true).serialize(destinationTopic, Headers.empty, key)
                  v <- getSerializer[F, GenericRecord](s)(isKey = false).serialize(destinationTopic, Headers.empty, value.orNull)
                } yield ProducerRecord(destinationTopic, k, v)).map(p => ProducerRecords.one(p))
              case None => fs2.Stream.empty
            }
          case _ =>
            fs2.Stream.empty
        }
      }
      // TODO Use passthrough to commit offset
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
      (topic: String, data: A) => serializer.serialize(topic, data)
    }.suspend

  private def getConsumerGroupDeserializer[F[_]: Sync, A](byteBufferToA: ByteBuffer => A): Deserializer[F, Option[A]] =
    Deserializer.delegate[F, Option[A]] {
      (_: String, data: Array[Byte]) => {
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

