package hydra.kafka.algebras

import java.nio.ByteBuffer
import java.time.Instant

import cats.effect.concurrent.{Deferred, Ref, Semaphore}
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import cats.{Applicative, MonadError, Order, data}
import fs2.Chunk.Bytes
import fs2.kafka._
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.ConsumerGroupsAlgebra.{Consumer, ConsumerTopics, Topic, TopicConsumers}
import hydra.kafka.algebras.KafkaClientAlgebra.{Offset, OffsetInfo, Partition, Record}
import hydra.kafka.model.TopicConsumer.{TopicConsumerKey, TopicConsumerValue}
import hydra.kafka.model.TopicConsumerOffset.{TopicConsumerOffsetKey, TopicConsumerOffsetValue}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model.{TopicConsumer, TopicConsumerOffset}
import io.chrisdavenport.log4cats.Logger
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import kafka.common.OffsetAndMetadata
import kafka.coordinator.group.{BaseKey, GroupMetadataManager, OffsetKey}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.util.Try

trait ConsumerGroupsAlgebra[F[_]] {
  def getConsumersForTopic(topicName: String): F[TopicConsumers]
  def getTopicsForConsumer(consumerGroupName: String): F[ConsumerTopics]
  def getAllConsumers: F[List[ConsumerTopics]]
}

object ConsumerGroupsAlgebra {

  type PartitionOffsetMap = Map[Int, Long]

  final case class TopicConsumers(topicName: String, consumers: List[Consumer])
  final case class Consumer(consumerGroupName: String, lastCommit: Instant)

  final case class ConsumerTopics(consumerGroupName: String, topics: List[Topic])
  final case class Topic(topicName: String, lastCommit: Instant)


  def make[F[_]: ContextShift: ConcurrentEffect: Timer: Logger](
                                                                 kafkaInternalTopic: String,
                                                                 dvsConsumersTopic: Subject,
                                                                 consumerOffsetsOffsetsTopicConfig: Subject, // __consumer_offsets is the internal kafka topic we're reading off of
                                                                 bootstrapServers: String,
                                                                 uniquePerNodeConsumerGroup: String,
                                                                 commonConsumerGroup: String,
                                                                 kafkaClientAlgebra: KafkaClientAlgebra[F],
                                                                 kafkaAdminAlgebra: KafkaAdminAlgebra[F],
                                                                 schemaRegistryAlgebra: SchemaRegistry[F]): F[ConsumerGroupsAlgebra[F]] = {
    val dvsConsumersStream: fs2.Stream[F, (GenericRecord, Option[GenericRecord])] = kafkaClientAlgebra.consumeMessages(dvsConsumersTopic.value, uniquePerNodeConsumerGroup, commitOffsets = false)
    val dvsConsumerOffsetStream = kafkaClientAlgebra.consumeMessagesWithOffsetInfo(consumerOffsetsOffsetsTopicConfig.value, uniquePerNodeConsumerGroup, commitOffsets = false)

    for {
      consumerGroupsStorageFacade <- Ref[F].of(ConsumerGroupsStorageFacade.empty)
      schemaRegistryClient <- schemaRegistryAlgebra.getSchemaRegistryClient
      latestOffsets <- kafkaAdminAlgebra.getLatestOffsets(consumerOffsetsOffsetsTopicConfig.value)
      simplifiedLatestOffsets = latestOffsets.map(l => l._1.partition -> l._2.value)
      deferred <- Deferred[F, PartitionOffsetMap]
      cache <- Ref[F].of(Map[Int, Long]())
      backgroundProcess <- Concurrent[F].start(getOffsetsToSeekTo(cache, simplifiedLatestOffsets, deferred, dvsConsumerOffsetStream))
      partitionMap <- deferred.get
      _ <- backgroundProcess.cancel
      _ <- Concurrent[F].start(consumerOffsetsToInternalOffsets(kafkaInternalTopic, dvsConsumersTopic.value, bootstrapServers, commonConsumerGroup, schemaRegistryClient, partitionMap, consumerOffsetsOffsetsTopicConfig.value))
      _ <- Concurrent[F].start(consumeDVSConsumersTopicIntoCache(dvsConsumersStream, consumerGroupsStorageFacade))
    } yield new ConsumerGroupsAlgebra[F] {
      override def getConsumersForTopic(topicName: String): F[TopicConsumers] =
        consumerGroupsStorageFacade.get.map(_.getConsumersForTopicName(topicName))

      override def getTopicsForConsumer(consumerGroupName: String): F[ConsumerTopics] =
        consumerGroupsStorageFacade.get.map(_.getTopicsForConsumerGroupName(consumerGroupName))

      override def getAllConsumers: F[List[ConsumerTopics]] =
        consumerGroupsStorageFacade.get.map(_.getAllConsumers)
    }
  }

  private[algebras] def getOffsetsToSeekTo[F[_]: ConcurrentEffect](
                                                          cache: Ref[F, PartitionOffsetMap],
                                                          latestPartitionOffset: PartitionOffsetMap,
                                                          deferred: Deferred[F, PartitionOffsetMap],
                                                          dvsConsumerOffsetStream: fs2.Stream[F, (Record, OffsetInfo)]
                                                        ): F[Unit] = {
    def onStart = if (latestPartitionOffset.values.forall(_ == 0L)) deferred.complete(Map()) else ConcurrentEffect[F].unit
    def isComplete: F[Unit] = for {
      map <- cache.get
      isFulfilled = map.keys.size == latestPartitionOffset.values.count(_ > 0)
      _ <- if (isFulfilled) {
        deferred.complete(map)
      } else {
        ConcurrentEffect[F].unit
      }
    } yield ()

    onStart *> dvsConsumerOffsetStream.flatMap { case ((key, value), (partition, offset)) =>
      fs2.Stream.eval(TopicConsumerOffset.decode[F](key, value).flatMap { case (topicKey, topicValue) =>
        if (latestPartitionOffset.get(partition).map(_ - 1).contains(offset) && topicValue.isDefined) {
          cache.update(_ + (topicKey.partition -> topicValue.get.offset))
        } else {
          ConcurrentEffect[F].unit
        }
      }).flatTap { _ =>
        fs2.Stream.eval(isComplete)
      }
    }.compile.drain
  }

  private def seekToLatestOffsets[F[_]: ConcurrentEffect : Logger](sourceTopic: String)
                                                                  (
                                                           stream: fs2.Stream[F, KafkaConsumer[F, Option[BaseKey], Option[OffsetAndMetadata]]],
                                                           p: PartitionOffsetMap
                                                         ): fs2.Stream[F, KafkaConsumer[F, Option[BaseKey], Option[OffsetAndMetadata]]] = {
    implicit val order: Order[TopicPartition] =
      (x: TopicPartition, y: TopicPartition) => if (x.partition() > y.partition()) 1 else if (x.partition() < y.partition()) -1 else 0
    stream.flatTap { b =>
      fs2.Stream.eval {
         b.subscribeTo(sourceTopic)
      }
    }
  }

  private def processRecord[F[_]: ConcurrentEffect](
                                                     cr: CommittableConsumerRecord[F, Option[BaseKey], Option[OffsetAndMetadata]],
                                                     s: SchemaRegistryClient,
                                                     destinationTopic: String,
                                                     dvsInternalKafkaOffsetTopic: String
                                                   ): fs2.Stream[F, ProducerRecords[Array[Byte], Array[Byte], Unit]] = {
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
              topicConsumerOffset <- TopicConsumerOffset.encode[F](
                TopicConsumerOffsetKey(cr.offset.topicPartition.topic(),cr.offset.topicPartition.partition()),
                TopicConsumerOffsetValue(cr.offset.offsetAndMetadata.offset())
              )
              (key, value) = topicConsumer
              (offsetKey, offsetValue) = topicConsumerOffset
              k <- getSerializer[F, GenericRecord](s)(isKey = true).serialize(destinationTopic, Headers.empty, key)
              v <- getSerializer[F, GenericRecord](s)(isKey = false).serialize(destinationTopic, Headers.empty, value.orNull)
              offsetK <- getSerializer[F, GenericRecord](s)(isKey = true).serialize(dvsInternalKafkaOffsetTopic, Headers.empty, offsetKey)
              offsetV <- getSerializer[F, GenericRecord](s)(isKey = false).serialize(dvsInternalKafkaOffsetTopic, Headers.empty, offsetValue)
            } yield  {
              val p = ProducerRecord(destinationTopic, k, v)
              val p2 = ProducerRecord(dvsInternalKafkaOffsetTopic, offsetK, offsetV)
              ProducerRecords(List(p, p2))
            })
          case None =>
            fs2.Stream.empty
        }
      case _ =>
        fs2.Stream.empty
    }
  }

  private def consumerOffsetsToInternalOffsets[F[_]: ConcurrentEffect: ContextShift: Timer: Logger]
  (
    sourceTopic: String,
    destinationTopic: String,
    bootstrapServers: String,
    consumerGroupName: String,
    s: SchemaRegistryClient,
    partitionMap: PartitionOffsetMap,
    dvsInternalKafkaOffsetTopic: String
  ) = {
    val settings: ConsumerSettings[F, Option[BaseKey], Option[OffsetAndMetadata]] = ConsumerSettings(
      getConsumerGroupDeserializer[F, BaseKey](GroupMetadataManager.readMessageKey),
      getConsumerGroupDeserializer[F, OffsetAndMetadata](GroupMetadataManager.readOffsetMessageValue)
    )
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(consumerGroupName)

    val producerSettings = ProducerSettings[F, Array[Byte], Array[Byte]]
      .withBootstrapServers(bootstrapServers)
      .withAcks(Acks.All)
    val consumer = consumerStream(settings)
    seekToLatestOffsets(sourceTopic)(consumer, partitionMap)
      .flatMap(_.stream)
      .flatMap { cr =>
        processRecord(cr, s, destinationTopic, dvsInternalKafkaOffsetTopic)
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
      (topic: String, data: A) => serializer.serialize(topic, data)
    }.suspend

  private def getConsumerGroupDeserializer[F[_]: Sync, A](byteBufferToA: ByteBuffer => A): Deserializer[F, Option[A]] =
    Deserializer.delegate[F, Option[A]] {
      (_: String, data: Array[Byte]) => {
        Try(byteBufferToA(Bytes(data).toByteBuffer)).toOption
      }
    }.suspend

  private def consumeDVSConsumersTopicIntoCache[F[_]: ContextShift: ConcurrentEffect: Timer: Logger](
                                                                                                      dvsConsumersStream: fs2.Stream[F, Record],
                                                                                                      consumerGroupsStorageFacade: Ref[F, ConsumerGroupsStorageFacade]
                                                                                                    ): F[Unit] = {
    dvsConsumersStream.flatMap { case (key, value) =>
      fs2.Stream.eval(TopicConsumer.decode[F](key, value).flatMap { case (topicKey, topicValue) =>
        topicValue match {
          case Some(tV) =>
            consumerGroupsStorageFacade.update(_.addConsumerGroup(topicKey, tV))
          case None =>
            consumerGroupsStorageFacade.update(_.removeConsumerGroup(topicKey))
        }
      })
    }.compile.drain
  }
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
  def getAllConsumers: List[ConsumerTopics] = {
    consumerMap.keys.map(_.consumerGroupName).toSet.map(getTopicsForConsumerGroupName).toList
  }
}

private object ConsumerGroupsStorageFacade {
  def empty: ConsumerGroupsStorageFacade = ConsumerGroupsStorageFacade(Map.empty)
}

