package hydra.kafka.algebras

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import hydra.kafka.algebras.KafkaClientAlgebra.{ConsumerGroup, TopicName}
import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataV2Container
//import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataV2
import hydra.kafka.model.{TopicMetadataV2, TopicMetadataV2Key, TopicMetadataV2Value}

trait MetadataAlgebra[F[_]] {

  import MetadataAlgebra._

  def getMetadata(topic: TopicName): F[Option[TopicMetadataV2Container]]

  def getAllMetadata: F[List[TopicMetadataV2Container]]

}

object MetadataAlgebra {

  final case class TopicMetadataV2Container(key: TopicMetadataV2Key, value: TopicMetadataV2Value)

  def live[F[_]: Sync: Concurrent](
                        metadataTopicName: TopicName,
                        consumerGroup: ConsumerGroup,
                        kafkaClientAlgebra: KafkaClientAlgebra[F]
                      ): F[MetadataAlgebra[F]] = {
    val metadataStream = kafkaClientAlgebra.consumeMessages(metadataTopicName, consumerGroup)
    for {
      ref <- Ref[F].of(MetadataStorageFacade.empty)
      _ <- Concurrent[F].start(metadataStream.flatMap { case (key, value) =>
        fs2.Stream.eval(TopicMetadataV2.decode(key, value).flatMap { case (topicMetadataKey, topicMetadataValue) =>
          ref.getAndUpdate(_.addMetadata(TopicMetadataV2Container(topicMetadataKey, topicMetadataValue)))
        })
      }.compile.drain)
      algebra <- getMetadataAlgebra[F](ref)
    } yield algebra
  }

  def test[F[_]: Sync](ref: Ref[F, MetadataStorageFacade]): F[MetadataAlgebra[F]] = {
    for {
      algebra <- getMetadataAlgebra[F](ref)
    } yield algebra
  }

  private def getMetadataAlgebra[F[_]: Sync](cache: Ref[F, MetadataStorageFacade]): F[MetadataAlgebra[F]] =
    Sync[F].delay {
      new MetadataAlgebra[F] {
        override def getMetadata(topic: TopicName): F[Option[TopicMetadataV2Container]] =
          cache.get.map(_.getMetadataByTopicName(topic))

        override def getAllMetadata: F[List[TopicMetadataV2Container]] =
          cache.get.map(_.getAllMetadata)
      }
    }
}

private case class MetadataStorageFacade(metadataMap: Map[TopicName, TopicMetadataV2Container]) {
  def getMetadataByTopicName(topicName: TopicName): Option[TopicMetadataV2Container] = metadataMap.get(topicName)
  def getAllMetadata: List[TopicMetadataV2Container] = metadataMap.values.toList
  def addMetadata(metadata: TopicMetadataV2Container): MetadataStorageFacade =
    this.copy(this.metadataMap + (metadata.key.subject.value -> metadata))
}

private object MetadataStorageFacade {
  def empty: MetadataStorageFacade = MetadataStorageFacade(Map.empty)
}
