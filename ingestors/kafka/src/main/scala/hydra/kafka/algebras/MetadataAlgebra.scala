package hydra.kafka.algebras

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.KafkaClientAlgebra.{ConsumerGroup, TopicName}
import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataV2Container
import hydra.kafka.model.TopicMetadataV2Transport.Subject
import hydra.kafka.model.{TopicMetadataV2, TopicMetadataV2Key, TopicMetadataV2Value}
import org.apache.avro.generic.GenericRecord

trait MetadataAlgebra[F[_]] {

  import MetadataAlgebra._

  def getMetadataFor(subject: Subject): F[Option[TopicMetadataV2Container]]

  def getAllMetadata: F[List[TopicMetadataV2Container]]

}

object MetadataAlgebra {

  final case class TopicMetadataV2Container(key: TopicMetadataV2Key, value: Option[TopicMetadataV2Value])

  def make[F[_]: Sync: Concurrent](
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
          TopicMetadataV2.decode[F](key, value).flatMap { case (topicMetadataKey, topicMetadataValue) =>
            schemaRegistryAlgebra.getLatestSchemaBySubject(subject = topicMetadataKey.subject.value + "-key").flatMap { keyschema =>
              schemaRegistryAlgebra.getLatestSchemaBySubject(subject = topicMetadataKey.subject.value + "-value").flatMap { valueSchema =>
                ref.update(_.addMetadata(TopicMetadataV2Container(topicMetadataKey, topicMetadataValue)))
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
        override def getMetadataFor(subject: Subject): F[Option[TopicMetadataV2Container]] =
          cache.get.map(_.getMetadataByTopicName(subject))

        override def getAllMetadata: F[List[TopicMetadataV2Container]] =
          cache.get.map(_.getAllMetadata)
      }
    }
  }
}

private case class MetadataStorageFacade(metadataMap: Map[Subject, TopicMetadataV2Container]) {
  def getMetadataByTopicName(subject: Subject): Option[TopicMetadataV2Container] = metadataMap.get(subject)
  def getAllMetadata: List[TopicMetadataV2Container] = metadataMap.values.toList
  def addMetadata(metadata: TopicMetadataV2Container): MetadataStorageFacade =
    this.copy(this.metadataMap + (metadata.key.subject -> metadata))
}

private object MetadataStorageFacade {
  def empty: MetadataStorageFacade = MetadataStorageFacade(Map.empty)
}
