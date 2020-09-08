package hydra.ingest.programs

import cats.MonadError
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.SchemaVersion
import hydra.kafka.algebras.KafkaAdminAlgebra.{KafkaDeleteTopicErrorList}
import hydra.kafka.algebras.{KafkaAdminAlgebra}
import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._
import hydra.ingest.programs.TopicDeletionProgram.{FailureToDeleteSchemaVersion, SchemaDeleteTopicErrorList}


final class TopicDeletionProgram[F[_]: MonadError[*[_], Throwable]](kafkaClient: KafkaAdminAlgebra[F],
                                              schemaClient: SchemaRegistry[F]) {

  private def deleteFromSchemaRegistry(topicNames: List[String]): F[ValidatedNel[FailureToDeleteSchemaVersion, Unit]] = {
    // Try deleting both -key and -value
    topicNames.flatMap(topic => List(topic + "-key", topic + "-value")).traverse { subject =>
      // Delete all versions of the schema
      schemaClient.getAllVersions(subject)
        .flatMap(_.traverse(version => schemaClient.deleteSchemaOfVersion(subject, version)
          .attempt.map(_.leftMap(cause => FailureToDeleteSchemaVersion(version, subject, cause)).toValidatedNel)))
    }.map(_.flatten.combineAll)
  }

  def deleteTopic(topicNames: List[String]): F[ValidatedNel[DeleteTopicError, Unit]] = {
    // delete the topic from Kafka
    kafkaClient.deleteTopics(topicNames).flatMap { result =>
      // get the names of the topics that succeeded
      val topicsToDeleteSchemaFor = result match {
        case Right(_) => topicNames
        case Left(error) =>
          val failedTopicNames = error.errors.map(_.topicName).toList.toSet
          topicNames.toSet.diff(failedTopicNames).toList
      }
      // delete topics that succeeded being deleted in Kafka in SchemaRegistry
      deleteFromSchemaRegistry(topicsToDeleteSchemaFor).map(schemaResult =>
        schemaResult.toEither.leftMap(a => SchemaDeletionErrors(SchemaDeleteTopicErrorList(a)))
          // combine SchemaRegistry errors and Kafka Errors for return
          .toValidatedNel.combine(result.leftMap(KafkaDeletionErrors).toValidatedNel)
      )
    }
  }
}

object TopicDeletionProgram {
  final case class FailureToDeleteSchemaVersion(schemaVersion: SchemaVersion, subject: String, cause: Throwable)
    extends Exception(s"Failed to delete $schemaVersion for $subject", cause) {
    def errorMessage: String = s"$subject ${cause.getMessage}"
  }

  final case class SchemaDeleteTopicErrorList(errors: NonEmptyList[FailureToDeleteSchemaVersion])
    extends Exception (s"Topic(s) failed to delete:\n${errors.map(_.errorMessage).toList.mkString("\n")}")
}

sealed abstract class DeleteTopicError extends RuntimeException
final case class KafkaDeletionErrors(kafkaDeleteTopicErrorList: KafkaDeleteTopicErrorList) extends DeleteTopicError
final case class SchemaDeletionErrors(schemaDeleteTopicErrorList: SchemaDeleteTopicErrorList) extends DeleteTopicError
