package hydra.ingest.programs

import cats.Monad
import cats.MonadError
import cats.data.Validated.Invalid
import cats.effect.Sync
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.SchemaVersion
import hydra.common.util.Futurable
import hydra.kafka.algebras.KafkaAdminAlgebra.{KafkaDeleteTopicError, KafkaDeleteTopicErrorList}
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import cats.data.{NonEmptyChain, NonEmptyList, Validated, ValidatedNec, ValidatedNel}
import cats.implicits._
import cats.effect.concurrent.Ref
import cats.effect.{Async, Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import hydra.ingest.programs.TopicDeletionProgram.{FailureToDeleteSchemaVersion, SchemaDeleteTopicErrorList}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

final class TopicDeletionProgram[F[_]: MonadError[*[_], Throwable]](kafkaClient: KafkaAdminAlgebra[F],
                                              schemaClient: SchemaRegistry[F]) {

  private def deleteFromSchemaRegistry(topicNames: List[String]): F[ValidatedNel[FailureToDeleteSchemaVersion, Unit]] = {
    topicNames.flatMap(topic => List(topic + "-key", topic + "-value")).traverse { subject =>
      schemaClient.getAllVersions(subject)
        .flatMap(_.traverse(version => schemaClient.deleteSchemaOfVersion(subject, version)
          .attempt.map(_.leftMap(cause => FailureToDeleteSchemaVersion(version, subject, cause)).toValidatedNel)))
    }.map(_.flatten.combineAll)
  }

  def deleteTopic(topicNames: List[String]): F[ValidatedNel[DeleteTopicError, Unit]] = {
    kafkaClient.deleteTopics(topicNames).flatMap { result =>
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
