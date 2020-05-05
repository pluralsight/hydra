package hydra.kafka.algebras

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import fs2.concurrent.Queue
import fs2.kafka._
import hydra.avro.registry.SchemaRegistry
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NoStackTrace

trait KafkaClientAlgebra[F[_]] {
  import KafkaClientAlgebra._
  /**
    * Publishes the Hydra record to Kafka
    * @param record - the hydra record that is to be ingested in Kafka
    * @return Either[PublishError, Unit] - Unit is returned upon success, PublishError on failure.
    */
  def publishMessage(
    record: Record,
    topicName: TopicName
  ): F[Either[PublishError, Unit]]

  /**
    * Consume the Hydra record from Kafka.
    * Does not commit offsets. Each time function is called will return
    * @param topicName - topic name to consume
    * @param consumerGroup - group id for consume
    * @return Stream that results in tupled K and V
    */
  def consumeMessages(
     topicName: TopicName,
     consumerGroup: ConsumerGroup
   ): fs2.Stream[F, Record]

}

object KafkaClientAlgebra {

  type TopicName = String
  type ConsumerGroup = String
  sealed trait RecordKey
  final case class GenericRecordKey(value: GenericRecord) extends RecordKey
  final case class StringKey(value: String) extends RecordKey
  type Record = (RecordKey, GenericRecord)

  sealed abstract class PublishError(message: String)
    extends Exception(message)
      with Product
      with Serializable

  object PublishError {

    final case object Timeout
      extends PublishError("Timeout while ingesting message.")
        with NoStackTrace

  }


  private def getProducerQueue[F[_]: ConcurrentEffect: ContextShift]
  (bootstrapServers: String, schemaRegistryClient: SchemaRegistryClient): F[fs2.concurrent.Queue[F, (RecordKey, GenericRecord, TopicName, Deferred[F, Unit])]] = {
    import fs2.kafka._
    val producerSettings =
      ProducerSettings(keySerializer = getKeySerializer(schemaRegistryClient), valueSerializer = getGenericRecordSerializer(schemaRegistryClient))
        .withBootstrapServers(bootstrapServers)
    for {
      queue <- fs2.concurrent.Queue.unbounded[F, (RecordKey, GenericRecord, TopicName, Deferred[F, Unit])]
      _ <- Concurrent[F].start(queue.dequeue.map { payload =>
        val record = ProducerRecord(payload._3, payload._1, payload._2)
        ProducerRecords.one(record, payload._4)
      }.through(produce(producerSettings)).evalMap { i => i.passthrough.complete(()) }.compile.drain)
    } yield queue
  }

  def live[F[_]: ContextShift: ConcurrentEffect: Timer](
      bootstrapServers: String,
      schemaRegistryAlgebra: SchemaRegistry[F]
  ): F[KafkaClientAlgebra[F]] = schemaRegistryAlgebra.getSchemaRegistryClient.flatMap { schemaRegistryClient =>
    getProducerQueue[F](bootstrapServers, schemaRegistryClient).map { queue =>
      new KafkaClientAlgebra[F] {
        override def publishMessage(
                                     record: Record,
                                     topicName: TopicName
                                   ): F[Either[PublishError, Unit]] = {
          Deferred[F, Unit].flatMap { d =>
            queue.enqueue1((record._1, record._2, topicName, d)) *>
              Concurrent.timeoutTo[F, Either[PublishError, Unit]](d.get.map(Right(_)), 5.seconds, Sync[F].pure(Left(PublishError.Timeout)))
          }
        }

        override def consumeMessages(topicName: TopicName, consumerGroup: String): fs2.Stream[F, (RecordKey, GenericRecord)] = {
          val consumerSettings = ConsumerSettings(
            keyDeserializer = getKeyDeserializer(schemaRegistryClient)(isKey = true),
            valueDeserializer = getGenericRecordDeserializer(schemaRegistryClient)
          )
            .withAutoOffsetReset(AutoOffsetReset.Earliest)
            .withBootstrapServers(bootstrapServers)
            .withGroupId(consumerGroup)
          consumerStream(consumerSettings)
            .evalTap(_.subscribeTo(topicName))
            .flatMap(_.stream)
            .map { committable =>
              val r = committable.record
              (r.key, r.value)
            }
        }
      }
    }
  }

  def test[F[_]: Sync: Concurrent]: F[KafkaClientAlgebra[F]] = Ref[F].of(MockFS2Kafka.empty[F]).map { cache =>
    new KafkaClientAlgebra[F] {
      override def publishMessage(record: Record, topicName: TopicName): F[Either[PublishError, Unit]] = {
        cache.update(_.publishMessage(topicName, record)) *>
          cache.get.flatMap(_.getConsumerQueuesFor(topicName).traverse(_.enqueue1(record))) *>
          Sync[F].pure(Right(()))
      }

      def consumeMessages(topicName: TopicName, consumerGroup: ConsumerGroup): fs2.Stream[F, Record] = {
        fs2.Stream.force(for {
          queue <- createNewStreamOfQueue(cache, topicName)
          _ <- cache.update(_.addConsumerQueue(topicName, consumerGroup, queue))
        } yield queue.dequeue)
      }
    }
  }

  private def createNewStreamOfQueue[F[_]: Concurrent](cache: Ref[F, MockFS2Kafka[F]], topicName: TopicName): F[Queue[F, Record]] = {
    for {
      streamRecords <- cache.get.map(_.getStreamFor(topicName))
      newQueue <- fs2.concurrent.Queue.unbounded[F, Record]
      _ <- streamRecords.traverse(newQueue.enqueue1)
    } yield newQueue
  }

  private def getGenericRecordDeserializer[F[_]: Sync](schemaRegistryClient: SchemaRegistryClient): Deserializer[F, GenericRecord] =
    Deserializer.delegate[F, GenericRecord] {
      val deserializer = {
        val de = new KafkaAvroDeserializer(schemaRegistryClient)
        de.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "").asJava, false)
        de
      }
    (topic: TopicName, data: Array[Byte]) => {
      deserializer.deserialize(topic, data).asInstanceOf[GenericRecord]
    }
  }.suspend



  private def getKeyDeserializer[F[_]: Sync](schemaRegistryClient: SchemaRegistryClient)(isKey: Boolean = false): Deserializer[F, RecordKey] = {
    Deserializer.delegate[F, RecordKey] {
      val deserializer = {
        val de = new KafkaAvroDeserializer(schemaRegistryClient)
        de.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "").asJava, isKey)
        de
      }
      val stringDeserializer = new StringDeserializer
      (topic: TopicName, data: Array[Byte]) => {
        Try(deserializer.deserialize(topic, data).asInstanceOf[GenericRecord]).map(GenericRecordKey).getOrElse(StringKey(stringDeserializer.deserialize(topic, data)))
      }
    }.suspend
  }

  private def getKeySerializer[F[_]: Sync](schemaRegistryClient: SchemaRegistryClient): Serializer[F, RecordKey] =
    Serializer.delegate[F, RecordKey] {
      val serializer = {
        val se = new KafkaAvroSerializer(schemaRegistryClient)
        se.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "").asJava, true)
        se
      }
      val stringSerializer = new StringSerializer
      (topic: TopicName, data: RecordKey) => data match {
        case GenericRecordKey(g) => serializer.serialize(topic, g)
        case StringKey(s) => stringSerializer.serialize(topic, s)
      }
    }.suspend

  private def getGenericRecordSerializer[F[_]: Sync](schemaRegistryClient: SchemaRegistryClient): Serializer[F, GenericRecord] =
    Serializer.delegate[F, GenericRecord] {
      val serializer = new KafkaAvroSerializer(schemaRegistryClient)
      (topic: TopicName, data: GenericRecord) => {
        serializer.serialize(topic, data)
      }
    }.suspend

  private final case class MockFS2Kafka[F[_]](
                                                   private val topics: Map[TopicName, List[Record]],
                                                   consumerQueues: Map[(TopicName, ConsumerGroup), fs2.concurrent.Queue[F, Record]]
                                                 ) {
    def publishMessage(topicName: TopicName, record: Record): MockFS2Kafka[F] = {
      val updatedStream: List[Record] = this.topics.getOrElse(topicName, List.empty) :+ record
      this.copy(topics = this.topics + (topicName -> updatedStream))
    }

    def addConsumerQueue(topicName: TopicName, consumerGroup: ConsumerGroup, queue: Queue[F, Record]): MockFS2Kafka[F] = {
      this.copy(consumerQueues = this.consumerQueues + ((topicName, consumerGroup) -> queue))
    }

    def getConsumerQueuesFor(topicName: TopicName): List[Queue[F, Record]] = this.consumerQueues.toList.filter(_._1._1 == topicName).map(_._2)

    def getConsumerQueue(topicName: TopicName, consumerGroup: ConsumerGroup): Option[Queue[F, Record]] = this.consumerQueues.get((topicName, consumerGroup))

    def getStreamFor(topicName: TopicName): List[Record] = this.topics.getOrElse(topicName, List())
  }

  private object MockFS2Kafka {
    def empty[F[_]]: MockFS2Kafka[F] = MockFS2Kafka[F](Map.empty, Map.empty)
  }

}
