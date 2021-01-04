package hydra.kafka.algebras

import cats.{Applicative, Monad, MonadError}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.syntax.all._
import cats.{Monad, MonadError}
import fs2.Pipe
import fs2.concurrent.Queue
import fs2.kafka._
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.KafkaClientAlgebra.PublishError.{RecordTooLarge, TopicNotFoundInMetadata}
import io.chrisdavenport.log4cats.Logger
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

trait KafkaClientAlgebra[F[_]] {
  import KafkaClientAlgebra._
  /**
    * Publishes the Hydra record to Kafka
    *
    * @param record - the hydra record that is to be ingested in Kafka
    * @param topicName - topic name to produce to
    * @return Either[PublishError, Unit] - Unit is returned upon success, PublishError on failure.
    */
  def publishMessage(
    record: Record,
    topicName: TopicName,
  ): F[Either[PublishError, PublishResponse]]

  /**
    * Publishes string keyed messages for compatibility with Hydra V1
    * @param record - Record with a string key and an avro body
    * @param topicName - topic name to produce to
    * @return Either[PublishError, Unit] - Unit is returned upon success, PublishError on failure.
    */
  def publishStringKeyMessage(
                      record: StringRecord,
                      topicName: TopicName,
                    ): F[Either[PublishError, PublishResponse]]

  /**
    * Consume the Hydra record from Kafka.
    * Conditionally commits offsets. Each time function is called will return
    * @param topicName - topic name to consume
    * @param consumerGroup - group id for consume
    * @return Stream that results in tupled K and V
    */
  def consumeMessages(
     topicName: TopicName,
     consumerGroup: ConsumerGroup,
     commitOffsets: Boolean
   ): fs2.Stream[F, Record]

  /**
    * Consume the Hydra record from Kafka with Partition and Offset Info.
    * Conditionally commits offsets. Each time function is called will return
    * @param topicName - topic name to consume
    * @param consumerGroup - group id for consume
    * @return Stream that results in tupled K and V
    */
  def consumeMessagesWithOffsetInfo(
                       topicName: TopicName,
                       consumerGroup: ConsumerGroup,
                       commitOffsets: Boolean
                     ): fs2.Stream[F, (Record, OffsetInfo)]

  /**
    * Consume the Hydra record from Kafka.
    * Does not commit offsets. Each time function is called will return
    * @param topicName - topic name to consume
    * @param consumerGroup - group id for consume
    * @return Stream that results in tupled K and V
    */
  def consumeStringKeyMessages(
                       topicName: TopicName,
                       consumerGroup: ConsumerGroup,
                       commitOffsets: Boolean
                     ): fs2.Stream[F, StringRecord]

  /**
    * Sets a size limit for the Producer
    * @param sizeLimitBytes The largest number of allowable bytes in a record (inclusive)
    * @return The new algebra that rejects records above `sizeLimitBytes`
    */
  def withProducerRecordSizeLimit(sizeLimitBytes: Long): F[KafkaClientAlgebra[F]]
}

object KafkaClientAlgebra {

  type TopicName = String
  type ConsumerGroup = String
  private sealed trait RecordFormat
  private final case class GenericRecordFormat(value: GenericRecord) extends RecordFormat
  private final case class StringFormat(value: Option[String]) extends RecordFormat
  type Partition = Int
  type Offset = Long
  type OffsetInfo = (Partition, Offset)
  case object OffsetsNotCommittableInTest extends NoStackTrace
  final case class OffsetInfoNotRetrievableInTest() extends NoStackTrace
  type Record = (GenericRecord, Option[GenericRecord], Option[Headers])
  type StringRecord = (Option[String], Option[GenericRecord], Option[Headers])

  final case class PublishResponse(partition: Int, offset: Option[Long])
  object PublishResponse {
    def apply(partition: Int, offset: Long): PublishResponse =
      new PublishResponse(partition, if (offset < 0) None else offset.some)
  }

  sealed abstract class PublishError(message: String, cause: Option[Throwable])
    extends Exception(message, cause.orNull)
      with Product
      with Serializable

  object PublishError {

    case object Timeout
      extends PublishError("Timeout while ingesting message.", None)
        with NoStackTrace
    final case class RecordTooLarge(actualSize: Long, sizeLimit: Long)
      extends PublishError(s"Record was $actualSize bytes but the limit is $sizeLimit bytes.", None)
        with NoStackTrace
    final case class OtherPublishError(cause: Throwable)
      extends PublishError(cause.getMessage, cause.some)
    final case class TopicNotFoundInMetadata(topicName: String, timeout: FiniteDuration, cause: Throwable) extends
      PublishError(s"Topic $topicName was not found in metadata after ${timeout.toMillis} ms.", cause.some)
  }

  private def checkSizeLimit[F[_]: MonadError[*[_], Throwable]](k: Array[Byte], v: Option[Array[Byte]], sizeLimitBytes: Option[Long]): F[Unit] = {
    val recordLength = Option(k).getOrElse(Array.empty).length + v.getOrElse(Array.empty).length
    sizeLimitBytes match {
      case Some(limit) =>
        if (recordLength > limit) {
          MonadError[F, Throwable].raiseError(RecordTooLarge(recordLength, limit))
        } else {
          Monad[F].unit
        }
      case None => Monad[F].unit
    }
  }

  private[this] final case class ProduceRecordInfo[F[_]](
                                                          key: Option[Array[Byte]],
                                                          value: Option[Array[Byte]],
                                                          topicName: TopicName,
                                                          promise: Deferred[F, Either[PublishError, PublishResponse]],
                                                          headers: Headers
                                                        )

  private def attemptProduce[F[_], K, V, A](
                              settings: ProducerSettings[F, K, V],
                              producer: KafkaProducer[F, K, V]
                            )(
                              implicit F: Concurrent[F]
                            ): Pipe[F, (ProducerRecords[K, V, Unit], A), (Either[Throwable, ProducerResult[K, V, Unit]], A)] =
    _.evalMap(r => producer.produce(r._1).map(_ -> r._2)).mapAsync(settings.parallelism)(i => i._1.attempt.map(_ -> i._2))

  private def getProducerQueue[F[_]: ConcurrentEffect: ContextShift: Logger]
    (bootstrapServers: String, publishMaxBlockMs: FiniteDuration): F[fs2.concurrent.Queue[F, ProduceRecordInfo[F]]] = {
    import fs2.kafka._
    val producerSettings =
      ProducerSettings[F, Array[Byte], Array[Byte]]
        .withBootstrapServers(bootstrapServers)
        .withAcks(Acks.All)
        .withProperty("max.block.ms", publishMaxBlockMs.toMillis.toString)
        .withRetries(retries = 0)
    for {
      queue <- fs2.concurrent.Queue.unbounded[F, ProduceRecordInfo[F]]
      _ <- Concurrent[F].start {
        producerStream[F].using(producerSettings).flatMap { producer =>
          queue.dequeue.map { payload =>
            val record = ProducerRecord(payload.topicName, payload.key.orNull, payload.value.orNull).withHeaders(payload.headers)
            (ProducerRecords.one(record), payload)
          }.through(attemptProduce(producerSettings, producer))
            .flatMap {
              case (Right(result), payload) =>
                fs2.Stream.chunk(result.records).evalMap(r => payload.promise.complete(PublishResponse(r._2.partition, r._2.offset).asRight))
              case (Left(t: org.apache.kafka.common.errors.TimeoutException), payload) if t.getMessage.contains("not present in metadata after") =>
                fs2.Stream.eval(payload.promise.complete(PublishError.TopicNotFoundInMetadata(payload.topicName, publishMaxBlockMs, t).asLeft))
              case (Left(error), payload) =>
                fs2.Stream.eval(payload.promise.complete(PublishError.OtherPublishError(error).asLeft))
            }
        }.compile.drain
      }
    } yield queue
  }

  private def getLiveInstance[F[_]: ContextShift: ConcurrentEffect: Timer](bootstrapServers: String)
                                                                          (queue: fs2.concurrent.Queue[F, ProduceRecordInfo[F]],
                                                                           schemaRegistryClient: SchemaRegistryClient,
                                                                           keySerializer: Serializer[F, RecordFormat],
                                                                           valSerializer: Serializer[F, RecordFormat],
                                                                           sizeLimitBytes: Option[Long] = None,
                                                                           publishTimeoutDuration: FiniteDuration,
                                                                           publishMaxBlockMs: FiniteDuration
                                                                          ): F[KafkaClientAlgebra[F]] = Sync[F].delay {
    new KafkaClientAlgebra[F] {
      override def publishMessage(record: Record, topicName: TopicName): F[Either[PublishError, PublishResponse]] = {
        produceMessage[GenericRecord](record, topicName, GenericRecordFormat.apply, publishTimeoutDuration)
      }

      override def publishStringKeyMessage(record: StringRecord, topicName: TopicName): F[Either[PublishError, PublishResponse]] = {
        produceMessage[Option[String]](record, topicName, StringFormat.apply, publishTimeoutDuration)
      }

      override def consumeMessages(topicName: TopicName, consumerGroup: String, commitOffsets: Boolean): fs2.Stream[F, Record] = {
        consumeMessages[GenericRecord](getGenericRecordDeserializer(schemaRegistryClient)(isKey = true), consumerGroup, topicName, commitOffsets).map(c => (c._1, c._2, c._3))
      }

      override def consumeMessagesWithOffsetInfo(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[F, (Record, (Partition, Offset))] = {
        consumeMessages[GenericRecord](getGenericRecordDeserializer(schemaRegistryClient)(isKey = true), consumerGroup, topicName, commitOffsets).map(c => ((c._1, c._2, c._3),(c._4,c._5)))
      }

      override def consumeStringKeyMessages(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[F, StringRecord] = {
        consumeMessages[Option[String]](getStringKeyDeserializer, consumerGroup, topicName, commitOffsets).map(c => (c._1, c._2, c._3))
      }

      override def withProducerRecordSizeLimit(sizeLimitBytes: Long): F[KafkaClientAlgebra[F]] =
        getLiveInstance[F](bootstrapServers)(queue, schemaRegistryClient, keySerializer, valSerializer, sizeLimitBytes.some, publishTimeoutDuration, publishMaxBlockMs)

      private def produceMessage[A](
                                     record: (A, Option[GenericRecord], Option[Headers]),
                                     topicName: TopicName,
                                     convert: A => RecordFormat,
                                     timeoutDuration: FiniteDuration): F[Either[PublishError, PublishResponse]] = {
        val kafkaHeaders: Headers = record._3 match {
          case Some(headersExist) => headersExist
          case _ => Headers.empty
        }
        for {
          d <- Deferred[F, Either[PublishError, PublishResponse]]
          k <- keySerializer.serialize(topicName, kafkaHeaders, convert(record._1))
          v <- record._2.traverse(r => valSerializer.serialize(topicName, kafkaHeaders, GenericRecordFormat(r)))
          _ <- checkSizeLimit[F](k, v, sizeLimitBytes)
          _ <- queue.enqueue1(ProduceRecordInfo(k.some, v, topicName, d, kafkaHeaders))
          resolve <- Concurrent.timeoutTo[F, Either[PublishError, PublishResponse]](d.get, timeoutDuration, Sync[F].pure(Left(PublishError.Timeout)))
        } yield resolve
      }

      private def consumeMessages[A](
                                      keyDeserializer: Deserializer[F, A],
                                      consumerGroup: ConsumerGroup,
                                      topicName: TopicName,
                                      commitOffsets: Boolean
                                    ): fs2.Stream[F, (A, Option[GenericRecord], Option[Headers], Partition, Offset)] = {
        val consumerSettings: ConsumerSettings[F, A, Option[GenericRecord]] = ConsumerSettings(
          keyDeserializer = keyDeserializer,
          valueDeserializer = getOptionalGenericRecordDeserializer(schemaRegistryClient)()
        )
          .withAutoOffsetReset(AutoOffsetReset.Earliest)
          .withBootstrapServers(bootstrapServers)
          .withGroupId(consumerGroup)
        consumerStream(consumerSettings)
          .evalTap(_.subscribeTo(topicName))
          .flatMap(_.stream)
          .evalTap{ committable =>
            if (commitOffsets) committable.offset.commit else Applicative[F].pure(())
          }
          .map { committable =>
            val r = committable.record
            val headers = if (r.headers.isEmpty) None else Option(r.headers)
            (r.key, r.value, headers, r.partition, r.offset)
          }
      }

    }
  }

  def live[F[_]: ContextShift: ConcurrentEffect: Timer: Logger](
      bootstrapServers: String,
      schemaRegistryAlgebra: SchemaRegistry[F],
      recordSizeLimit: Option[Long] = None,
      publishTimeoutDuration: FiniteDuration = 5.seconds,
      publishMaxBlockMs: FiniteDuration = 4.5.seconds
  ): F[KafkaClientAlgebra[F]] =
    for {
      schemaRegistryClient <- schemaRegistryAlgebra.getSchemaRegistryClient
      queue <- getProducerQueue[F](bootstrapServers, publishMaxBlockMs)
      k <- getLiveInstance(bootstrapServers)(
        queue, schemaRegistryClient,
        getSerializer(schemaRegistryClient)(isKey = true),
        getSerializer(schemaRegistryClient)(isKey = false), None, publishTimeoutDuration, publishMaxBlockMs)
      kWithSizeLimit <- recordSizeLimit.traverse(k.withProducerRecordSizeLimit)
    } yield kWithSizeLimit.getOrElse(k)

  final case class ConsumeErrorException(message: String) extends Exception(message)
  private def getTestInstance[F[_]: Sync: Concurrent](cache: Ref[F, MockFS2Kafka[F]],
                                                      schemaRegistry: SchemaRegistry[F],
                                                      sizeLimitBytes: Option[Long] = None): KafkaClientAlgebra[F] = new KafkaClientAlgebra[F] {

    override def publishMessage(record: Record, topicName: TopicName): F[Either[PublishError, PublishResponse]] = {
      val cacheRecord = (GenericRecordFormat(record._1), record._2, Some(record._3.getOrElse(Headers.empty)))
      publishCacheMessage(cacheRecord, topicName)
    }

    override def publishStringKeyMessage(record: StringRecord, topicName: TopicName): F[Either[PublishError, PublishResponse]] = {
      val cacheRecord = (StringFormat(record._1), record._2, record._3)
      publishCacheMessage(cacheRecord, topicName)
    }

    override def consumeMessages(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[F, Record] = {
      if (commitOffsets) fs2.Stream.raiseError[F](OffsetsNotCommittableInTest)
      else {
        consumeCacheMessage(topicName, consumerGroup).evalMap {
        case (r: GenericRecordFormat, v, h) => {
          val headers = h match {
            case Some(value) => if (value.isEmpty) None else Some(value)
            case _ => None
          }
          Sync[F].pure((r.value, v, headers))
        }
          case _ => Sync[F].raiseError[Record](ConsumeErrorException("Expected GenericRecord, got String"))
      }
    }
}

    override def consumeStringKeyMessages(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[F, StringRecord] = {
      if (commitOffsets) fs2.Stream.raiseError[F](OffsetsNotCommittableInTest)
      else {
        consumeCacheMessage(topicName, consumerGroup).evalMap {
          case (r: StringFormat, v, h) => {
          val headers = h match {
            case Some(value) => if (value.isEmpty) None else Some(value)
            case _ => None
          }
			Sync[F].pure((r.value, v, headers))
		  }
          case _ => Sync[F].raiseError[StringRecord](ConsumeErrorException("Expected String, got GenericRecord"))
        }
      }
    }

    override def withProducerRecordSizeLimit(sizeLimitBytes: Long): F[KafkaClientAlgebra[F]] = Sync[F].delay {
      getTestInstance(cache, schemaRegistry, sizeLimitBytes.some)
    }

    private def consumeCacheMessage(topicName: TopicName, consumerGroup: ConsumerGroup): fs2.Stream[F, CacheRecord] = {
      fs2.Stream.force(for {
        queue <- createNewStreamOfQueue(cache, topicName)
        _ <- cache.update(_.addConsumerQueue(topicName, consumerGroup, queue))
      } yield queue.dequeue)
    }

    private def checkSize(cacheRecord: CacheRecord, topicName: TopicName): F[Unit] = {
      for {
        keySerializer <- schemaRegistry.getSchemaRegistryClient.map(getSerializer(_)(isKey = true))
        valSerializer <- schemaRegistry.getSchemaRegistryClient.map(getSerializer(_)(isKey = false))
        key <- keySerializer.serialize(topicName, Headers.empty, cacheRecord._1)
        value <- cacheRecord._2.traverse(gr => valSerializer.serialize(topicName, Headers.empty, GenericRecordFormat(gr)))
        _ <- checkSizeLimit[F](key, value, sizeLimitBytes)
      } yield ()
    }

    private def publishCacheMessage(cacheRecord: CacheRecord, topicName: TopicName): F[Either[PublishError, PublishResponse]] = {
      checkSize(cacheRecord, topicName) *> cache.modify { c =>
        (c.publishMessage(topicName, cacheRecord), c.getStreamFor(topicName).length)
      }.flatMap { offset =>
        cache.get.flatMap(_.getConsumerQueuesFor(topicName).traverse(_.enqueue1(cacheRecord))) *>
          Sync[F].pure(Right(PublishResponse(0, offset)))
      }
    }

    override def consumeMessagesWithOffsetInfo(topicName: TopicName, consumerGroup: ConsumerGroup, commitOffsets: Boolean): fs2.Stream[F, ((GenericRecord, Option[GenericRecord], Option[Headers]), (Partition, Offset))] = {
      fs2.Stream.raiseError[F](OffsetInfoNotRetrievableInTest())
    }
  }

  def test[F[_]: Sync: Concurrent]: F[KafkaClientAlgebra[F]] = SchemaRegistry.test[F].flatMap { sr =>
    test(sr)
  }

  def test[F[_]: Sync: Concurrent](schemaRegistry: SchemaRegistry[F]): F[KafkaClientAlgebra[F]] = Ref[F].of(MockFS2Kafka.empty[F]).map { cache =>
    getTestInstance(cache, schemaRegistry)
  }

  private def createNewStreamOfQueue[F[_]: Concurrent](cache: Ref[F, MockFS2Kafka[F]], topicName: TopicName): F[Queue[F, CacheRecord]] = {
    for {
      streamRecords <- cache.get.map(_.getStreamFor(topicName))
      newQueue <- fs2.concurrent.Queue.unbounded[F, CacheRecord]
      _ <- streamRecords.traverse(newQueue.enqueue1)
    } yield newQueue
  }

  private def getStringKeyDeserializer[F[_]: Sync]: Deserializer[F, Option[String]] = {
    Deserializer.delegate[F, Option[String]] {
      val stringDeserializer = new StringDeserializer
      (topic: TopicName, data: Array[Byte]) => {
        Option(stringDeserializer.deserialize(topic, data))
      }
    }.suspend
  }

  private def getGenericRecordDeserializer[F[_]: Sync](schemaRegistryClient: SchemaRegistryClient)(isKey: Boolean = false): Deserializer[F, GenericRecord] =
    Deserializer.delegate[F, GenericRecord] {
      val deserializer = {
        val de = new KafkaAvroDeserializer(schemaRegistryClient)
        de.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "").asJava, isKey)
        de
      }
      (topic: TopicName, data: Array[Byte]) => {
        deserializer.deserialize(topic, data).asInstanceOf[GenericRecord]
      }
    }.suspend

  private def getOptionalGenericRecordDeserializer[F[_]: Sync](schemaRegistryClient: SchemaRegistryClient)(isKey: Boolean = false): Deserializer[F, Option[GenericRecord]] =
    Deserializer.delegate[F, Option[GenericRecord]] {
      val deserializer = {
        val de = new KafkaAvroDeserializer(schemaRegistryClient)
        de.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "").asJava, isKey)
        de
      }
    (topic: TopicName, data: Array[Byte]) => {
      Option(data).map(deserializer.deserialize(topic, _).asInstanceOf[GenericRecord])
    }
  }.suspend

  private def getSerializer[F[_]: Sync](schemaRegistryClient: SchemaRegistryClient)(isKey: Boolean): Serializer[F, RecordFormat] =
    Serializer.delegate[F, RecordFormat] {
      val serializer = {
        val se = new KafkaAvroSerializer(schemaRegistryClient)
        se.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "").asJava, isKey)
        se
      }
      val stringSerializer = new StringSerializer
      (topic: TopicName, data: RecordFormat) => data match {
        case GenericRecordFormat(g) => serializer.serialize(topic, g)
        case StringFormat(s) => s.map(stringSerializer.serialize(topic, _)).orNull
      }
    }.suspend

  private type CacheRecord = (RecordFormat, Option[GenericRecord], Option[Headers])

  private final case class MockFS2Kafka[F[_]](
                                                   private val topics: Map[TopicName, List[CacheRecord]],
                                                   consumerQueues: Map[(TopicName, ConsumerGroup), fs2.concurrent.Queue[F, CacheRecord]]
                                                 ) {
    def publishMessage(topicName: TopicName, record: CacheRecord): MockFS2Kafka[F] = {
      val updatedStream: List[CacheRecord] = this.topics.getOrElse(topicName, List.empty) :+ record
      this.copy(topics = this.topics + (topicName -> updatedStream))
    }

    def addConsumerQueue(topicName: TopicName, consumerGroup: ConsumerGroup, queue: Queue[F, CacheRecord]): MockFS2Kafka[F] = {
      this.copy(consumerQueues = this.consumerQueues + ((topicName, consumerGroup) -> queue))
    }

    def getConsumerQueuesFor(topicName: TopicName): List[Queue[F, CacheRecord]] = this.consumerQueues.toList.filter(_._1._1 == topicName).map(_._2)

    def getConsumerQueue(topicName: TopicName, consumerGroup: ConsumerGroup): Option[Queue[F, CacheRecord]] = this.consumerQueues.get((topicName, consumerGroup))

    def getStreamFor(topicName: TopicName): List[CacheRecord] = this.topics.getOrElse(topicName, List())
  }

  private object MockFS2Kafka {
    def empty[F[_]]: MockFS2Kafka[F] = MockFS2Kafka[F](Map.empty, Map.empty)
  }

}
