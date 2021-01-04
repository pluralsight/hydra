package hydra.kafka.algebras

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Sync}
import cats.syntax.all._
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.KafkaClientAlgebra.{ConsumerGroup, TopicName}
import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataContainer
import hydra.kafka.model.TopicMetadataV2.MetadataAvroSchemaFailure
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model.{TopicMetadataV2, TopicMetadataV2Key, TopicMetadataV2Request, TopicMetadataV2Value}
import org.apache.avro.generic.GenericRecord
import io.chrisdavenport.log4cats.Logger
import org.apache.avro.Schema


trait MetadataAlgebra[F[_]] {

  import MetadataAlgebra._

  def getMetadataFor(subject: Subject): F[Option[TopicMetadataContainer]]

  def getAllMetadata: F[List[TopicMetadataContainer]]

}

object MetadataAlgebra {

  final case class MetadataValueNotFoundException(message: String) extends Exception(message)
  final case class TopicMetadataContainer(key: TopicMetadataV2Key, value: TopicMetadataV2Value, keySchema: Option[Schema], valueSchema: Option[Schema])

  def make[F[_]: Sync: Concurrent: Logger](
                        metadataTopicName: TopicName,
                        consumerGroup: ConsumerGroup,
                        kafkaClientAlgebra: KafkaClientAlgebra[F],
                        schemaRegistryAlgebra: SchemaRegistry[F],
                        consumeMetadataEnabled: Boolean
                      ): F[MetadataAlgebra[F]] = {
    val metadataStream: fs2.Stream[F, (GenericRecord, Option[GenericRecord])] = if (consumeMetadataEnabled) {
      kafkaClientAlgebra.consumeMessages(metadataTopicName, consumerGroup, commitOffsets = false).map(record => (record._1, record._2))
    } else {
      fs2.Stream.empty
    }
    for {
      ref <- Ref[F].of(MetadataStorageFacade.empty)
      _ <- Concurrent[F].start(metadataStream.flatMap { case (key, value) =>
        fs2.Stream.eval {
          TopicMetadataV2.decode[F](key, value).flatMap { case (topicMetadataKey, topicMetadataValueOpt) =>
            topicMetadataValueOpt match {
              case Some(topicMetadataValue) =>
                schemaRegistryAlgebra.getLatestSchemaBySubject(subject = topicMetadataKey.subject.value + "-key").flatMap { keySchema =>
                  schemaRegistryAlgebra.getLatestSchemaBySubject(subject = topicMetadataKey.subject.value + "-value").flatMap { valueSchema =>
                    val topicMetadataV2Transport = TopicMetadataContainer(topicMetadataKey, topicMetadataValue, keySchema, valueSchema)
                    ref.update(_.addMetadata(topicMetadataV2Transport))
                  }
                }.recover {
                  case e =>
                    val topicMetadataV2Transport = TopicMetadataContainer(topicMetadataKey, topicMetadataValue, None, None)
                    Logger[F].error(s"Error retrieving Schema from SchemaRegistry on Kafka Read: ${e.getMessage}") *>
                    ref.update(_.addMetadata(topicMetadataV2Transport))
                }
              case None =>
                Logger[F].error("Metadata value not found")
            }
          }
        }.recoverWith {
          case e: MetadataAvroSchemaFailure =>
            fs2.Stream.eval(Logger[F].warn(s"Error in metadata consumer $e"))
        }
      }.compile.drain)
      algebra <- getMetadataAlgebra[F](ref, schemaRegistryAlgebra)
    } yield algebra
  }

  private def getMetadataAlgebra[F[_]: Sync: Logger](cache: Ref[F, MetadataStorageFacade], schemaRegistryAlgebra: SchemaRegistry[F]): F[MetadataAlgebra[F]] = {
    Sync[F].delay {
      new MetadataAlgebra[F] {
        override def getMetadataFor(subject: Subject): F[Option[TopicMetadataContainer]] =
          cache.get.map(_.getMetadataByTopicName(subject)).flatMap {
            case Some(t) if t.keySchema.isEmpty | t.valueSchema.isEmpty =>
              updateCacheWithNewSchemaRegistryValues(t).map(Some(_))
            case a => Sync[F].pure(a)
          }

        override def getAllMetadata: F[List[TopicMetadataContainer]] =
          cache.get.map(_.getAllMetadata).flatMap { metadata =>
            val (good2go, needs2beUpdated) = metadata.partition(m => m.keySchema.isDefined && m.valueSchema.isDefined)
            needs2beUpdated.traverse(updateCacheWithNewSchemaRegistryValues).map(_ ++ good2go)
          }

        /**
          * Updates TopicMetadataContainer with new values from SchemaRegistry
          * @param t - TopicMetadataContainer with either keySchema=None or valueSchema=None
          * @return TopicMetadataContainer with new Schemas from SchemaRegistry if they were undefined
          */
        private def updateCacheWithNewSchemaRegistryValues(t: TopicMetadataContainer): F[TopicMetadataContainer] = {
          schemaRegistryAlgebra.getLatestSchemaBySubject(subject = t.key.subject.value + "-key").flatMap { keySchema =>
            schemaRegistryAlgebra.getLatestSchemaBySubject(subject = t.key.subject.value + "-value").flatMap { valueSchema =>
              val updatedTopic = t.copy(keySchema = t.keySchema.orElse(keySchema), valueSchema = t.valueSchema.orElse(valueSchema))
              cache.update(_.addMetadata(updatedTopic)) *> Sync[F].pure(updatedTopic)
            }
          }.recover {
            case e =>
              Logger[F].error(s"Error retrieving Schema from SchemaRegistry: ${e.getMessage}")
              t
          }
        }
      }
    }
  }
}

private case class MetadataStorageFacade(metadataMap: Map[Subject, TopicMetadataContainer]) {
  def getMetadataByTopicName(subject: Subject): Option[TopicMetadataContainer] = metadataMap.get(subject)
  def getAllMetadata: List[TopicMetadataContainer] = metadataMap.values.toList
  def addMetadata(metadata: TopicMetadataContainer): MetadataStorageFacade =
    this.copy(this.metadataMap + (metadata.key.subject -> metadata))
}

private object MetadataStorageFacade {
  def empty: MetadataStorageFacade = MetadataStorageFacade(Map.empty)
}
