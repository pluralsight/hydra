package hydra.kafka.algebras

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.KafkaClientAlgebra.{ConsumerGroup, TopicName}
import hydra.kafka.model.TopicMetadataV2Transport.Subject
import hydra.kafka.model.{TopicMetadataV2, TopicMetadataV2Key, TopicMetadataV2Transport, TopicMetadataV2Value}
import org.apache.avro.generic.GenericRecord
import io.chrisdavenport.log4cats.Logger


trait MetadataAlgebra[F[_]] {

  import MetadataAlgebra._

  def getMetadataFor(subject: Subject): F[Option[TopicMetadataV2Transport]]

  def getAllMetadata: F[List[TopicMetadataV2Transport]]

}

object MetadataAlgebra {

  final case class MetadataValueNotFoundException(message: String) extends Exception(message)

  def make[F[_]: Sync: Concurrent: Logger](
                        metadataTopicName: TopicName,
                        consumerGroup: ConsumerGroup,
                        kafkaClientAlgebra: KafkaClientAlgebra[F],
                        schemaRegistryAlgebra: SchemaRegistry[F],
                        consumeMetadataEnabled: Boolean
                      ): F[MetadataAlgebra[F]] = {
    val metadataStream: fs2.Stream[F, (GenericRecord, Option[GenericRecord])] = if (consumeMetadataEnabled) {
      kafkaClientAlgebra.consumeMessages(metadataTopicName, consumerGroup)
    } else {
      fs2.Stream.empty
    }
    for {
      ref <- Ref[F].of(MetadataStorageFacade.empty)
      _ <- Concurrent[F].start(metadataStream.flatMap { case (key, value) =>
        fs2.Stream.eval {
          TopicMetadataV2.decode[F](key, value).flatMap { case (topicMetadataKey, topicMetadataValueOpt) =>
            schemaRegistryAlgebra.getLatestSchemaBySubject(subject = topicMetadataKey.subject.value + "-key").flatMap { keySchema =>
              schemaRegistryAlgebra.getLatestSchemaBySubject(subject = topicMetadataKey.subject.value + "-value").flatMap { valueSchema =>
                topicMetadataValueOpt match {
                  case Some(topicMetadataValue) =>
                    val topicMetadataV2Transport = TopicMetadataV2Transport.fromKeyAndValue(topicMetadataKey, topicMetadataValue, keySchema, valueSchema)
                    ref.update(_.addMetadata(topicMetadataV2Transport))
                  case None =>
                    Logger[F].error("Metadata value not found")
                }
              }
            }
          }
        }
      }.compile.drain)
      algebra <- getMetadataAlgebra[F](ref)
    } yield algebra
  }

  private def getMetadataAlgebra[F[_]: Sync](cache: Ref[F, MetadataStorageFacade]): F[MetadataAlgebra[F]] = {
    Sync[F].delay {
      new MetadataAlgebra[F] {
        override def getMetadataFor(subject: Subject): F[Option[TopicMetadataV2Transport]] =
          cache.get.map(_.getMetadataByTopicName(subject))

        override def getAllMetadata: F[List[TopicMetadataV2Transport]] =
          cache.get.map(_.getAllMetadata)
      }
    }
  }
}

private case class MetadataStorageFacade(metadataMap: Map[Subject, TopicMetadataV2Transport]) {
  def getMetadataByTopicName(subject: Subject): Option[TopicMetadataV2Transport] = metadataMap.get(subject)
  def getAllMetadata: List[TopicMetadataV2Transport] = metadataMap.values.toList
  def addMetadata(metadata: TopicMetadataV2Transport): MetadataStorageFacade =
    this.copy(this.metadataMap + (metadata.subject -> metadata))
}

private object MetadataStorageFacade {
  def empty: MetadataStorageFacade = MetadataStorageFacade(Map.empty)
}
