package hydra.kafka.algebras

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import hydra.kafka.algebras.MetadataAlgebra.{TopicMetadataV2, TopicName}
import hydra.kafka.model.{TopicMetadataV2Key, TopicMetadataV2Value}
import hydra.kafka.model.TopicMetadataV2Request.Subject

trait MetadataAlgebra[F[_]] {

  import MetadataAlgebra._

  def getMetadata(topic: TopicName): F[Option[TopicMetadataV2]]

  def getAllMetadata: F[List[TopicMetadataV2]]

}

object MetadataAlgebra {

  type TopicName = Subject

  final case class TopicMetadataV2(key: TopicMetadataV2Key, value: TopicMetadataV2Value)

  def live[F[_]: Sync](bootstrapServers: String, metadataTopicName: TopicName, consumerGroup: String, schemaRegistryUrl: String): F[MetadataAlgebra[F]] = {
    for {
      ref <- Ref[F].of(MetadataStorageFacade.empty)
      algebra <- getMetadataAlgebra[F](ref)
    } yield algebra
  }

  def test[F[_]: Sync]: F[MetadataAlgebra[F]] = {
    for {
      ref <- Ref[F].of(MetadataStorageFacade.empty)
      algebra <- getMetadataAlgebra[F](ref)
    } yield algebra
  }

  private def getMetadataAlgebra[F[_]: Sync](cache: Ref[F, MetadataStorageFacade]): F[MetadataAlgebra[F]] =
    Sync[F].delay {
      new MetadataAlgebra[F] {
        override def getMetadata(topic: TopicName): F[Option[TopicMetadataV2]] =
          cache.get.map(_.getMetadataByTopicName(topic))

        override def getAllMetadata: F[List[TopicMetadataV2]] =
          cache.get.map(_.getAllMetadata)
      }
    }
}

private case class MetadataStorageFacade(metadataMap: Map[TopicName, TopicMetadataV2]) {
  def getMetadataByTopicName(topicName: TopicName): Option[TopicMetadataV2] = metadataMap.get(topicName)
  def getAllMetadata: List[TopicMetadataV2] = metadataMap.values.toList
  def addMetadata(metadata: TopicMetadataV2): MetadataStorageFacade =
    this.copy(this.metadataMap + (metadata.key.subject -> metadata))
}

private object MetadataStorageFacade {
  def empty: MetadataStorageFacade = MetadataStorageFacade(Map.empty)
}
