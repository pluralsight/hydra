package hydra.ingest.programs

import cats.Monad
import cats.MonadError
import cats.effect.Sync
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.SchemaVersion
import hydra.common.util.Futurable
import hydra.kafka.algebras.KafkaAdminAlgebra.{DeleteTopicError, DeleteTopicErrorList}
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import cats.data.{NonEmptyChain, NonEmptyList, Validated, ValidatedNec, ValidatedNel}
import cats.implicits._
import cats.effect.concurrent.Ref
import cats.effect.{Async, Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import hydra.ingest.programs.TopicDeletionProgram.FailureToDeleteSchemaVersion

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

  def deleteTopic(topicNames: List[String]): F[Either[DeleteTopicErrorList, Unit]] = {
    kafkaClient.deleteTopics(topicNames).flatMap { result =>
      val topicsToDeleteSchemaFor = result match {
        case Right(_) => topicNames
        case Left(error) =>
          val failedTopicNames = error.errors.map(_.topicName).toList.toSet
          topicNames.toSet.diff(failedTopicNames).toList
      }
      // TODO chain errors together for failures from topic deletions and schema deletions
      // This will require a common super type for the failures (so they can be combined into a single list)
      deleteFromSchemaRegistry(topicsToDeleteSchemaFor)
    }
  }
}

object TopicDeletionProgram {
  final case class FailureToDeleteSchemaVersion(schemaVersion: SchemaVersion, subject: String, cause: Throwable)
    extends Exception(s"Failed to delete $schemaVersion for $subject", cause)
}
