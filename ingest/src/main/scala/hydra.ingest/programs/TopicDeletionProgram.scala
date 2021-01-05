package hydra.ingest.programs

import cats.MonadError
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.SchemaVersion
import hydra.kafka.algebras.KafkaAdminAlgebra.KafkaDeleteTopicErrorList
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._
import hydra.ingest.programs.TopicDeletionProgram.{MetadataFailToDelete, SchemaDeleteTopicErrorList, SchemaFailToDelete, SchemaRegistryError}
import hydra.kafka.algebras.KafkaClientAlgebra.PublishError
import hydra.kafka.model.{TopicMetadata, TopicMetadataV2, TopicMetadataV2Key}
import hydra.kafka.model.TopicMetadataV2Request.Subject


final class TopicDeletionProgram[F[_]: MonadError[*[_], Throwable]](kafkaAdmin: KafkaAdminAlgebra[F],
                                                                    kafkaClient: KafkaClientAlgebra[F],
                                                                    v2MetadataTopicName: Subject,
                                                                    v1MetadataTopicName: String,
                                              schemaClient: SchemaRegistry[F],
                                                                    topicMetadata: MetadataAlgebra[F]) {

  def deleteFromSchemaRegistry(topicNames: List[String]): F[ValidatedNel[SchemaRegistryError, Unit]] = {
    topicNames.flatMap(topic => List(topic + "-key", topic + "-value")).traverse { subject =>
      schemaClient.deleteSchemaSubject(subject).attempt.map {
        _.leftMap(error => SchemaFailToDelete(subject, error)).toValidatedNel
      }
    }.map(_.combineAll)
  }

  def deleteTopic(topicNames: List[String]): F[ValidatedNel[DeleteTopicError, Unit]] = {
    kafkaAdmin.deleteTopics(topicNames).flatMap { result =>
      val topicsToDeleteSchemaFor = result match {
        case Right(_) => topicNames
        case Left(error) =>
          val failedTopicNames = error.errors.map(_.topicName).toList.toSet
          topicNames.toSet.diff(failedTopicNames).toList
      }
      deleteFromSchemaRegistry(topicsToDeleteSchemaFor).map(schemaResult =>
        schemaResult.toEither.leftMap(a => SchemaDeletionErrors(SchemaDeleteTopicErrorList(a)))
          .toValidatedNel.combine(result.leftMap(KafkaDeletionErrors).toValidatedNel)
      )
    }
  }

  private def lookupAndDeleteMetadataForTopics(topicNames: List[String]): F[ValidatedNel[MetadataFailToDelete, Unit]] = {
    topicNames.traverse(topicName => {
      Subject.createValidated(topicName) match {
        case Some(subject) =>
          topicMetadata.getMetadataFor(subject).flatMap(metadata => {
            metadata match {
              case Some(_) =>
                deleteV2Metadata(subject).attempt.map {
                  _.leftMap(error => MetadataFailToDelete(topicName, v1MetadataTopicName, error)).toValidatedNel
                }
              case other =>
                deleteV1Metadata(topicName).attempt.map {
                  _.leftMap(error => MetadataFailToDelete(topicName, v1MetadataTopicName, error)).toValidatedNel
                }
            }
          })
        case other =>
          deleteV1Metadata(topicName).attempt.map {
            _.leftMap(error => MetadataFailToDelete(topicName, v1MetadataTopicName, error)).toValidatedNel
          }
      }
    }).map(_.combineAll)
  }

  private def deleteV2Metadata(topicName: Subject): F[Unit] = {
    for {
      records <- TopicMetadataV2.encode[F](TopicMetadataV2Key(topicName), None, None)
      _ <- kafkaClient
        .publishMessage(records, v2MetadataTopicName.value)
        .rethrow
    } yield ()
  }

  private def deleteV1Metadata(topicName: String): F[Unit] = {
    for {
      _ <- kafkaClient
          .publishStringKeyMessage((Some(topicName), None, None), v1MetadataTopicName)
          .rethrow
    } yield ()
  }
}

object TopicDeletionProgram {

  sealed abstract class SchemaRegistryError(subject: String, message: String, cause: Throwable) extends RuntimeException(message, cause) {
    def errorMessage: String = s"$message $cause"
    def getSubject: String = subject
  }

  final case class SchemaFailToDelete(subject: String, cause: Throwable)
    extends SchemaRegistryError(subject, s"Unable to delete schemas for $subject", cause)

  final case class SchemaDeleteTopicErrorList(errors: NonEmptyList[SchemaRegistryError])
    extends Exception (s"Topic(s) failed to delete:\n${errors.map(_.errorMessage).toList.mkString("\n")}")

  final case class MetadataFailToDelete(subject: String, metadataTopic: String, cause: Throwable)
    extends Exception(s"Unable to delete $subject from $metadataTopic", cause)
}

sealed abstract class DeleteTopicError extends RuntimeException
final case class KafkaDeletionErrors(kafkaDeleteTopicErrorList: KafkaDeleteTopicErrorList) extends DeleteTopicError
final case class SchemaDeletionErrors(schemaDeleteTopicErrorList: SchemaDeleteTopicErrorList) extends DeleteTopicError
