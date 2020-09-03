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

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

final class TopicDeletionProgram[F[_]: MonadError[*[_], Throwable]](kafkaClient: KafkaAdminAlgebra[F],
                                              schemaClient: SchemaRegistry[F]) {

  private def deleteVersionFromSchemaRegistry(schemaVersion: SchemaVersion, topic: String): F[Either[DeleteTopicError, Unit]] = {
    schemaClient.deleteSchemaOfVersion(topic, schemaVersion).attempt.map(_.leftMap(DeleteTopicError(topic,_)))
  }

  private def getVersionFromSchemaRegistry(topic: String): F[Either[DeleteTopicError, List[SchemaVersion]]] = {
    schemaClient.getAllVersions(topic).attempt.map(_.leftMap(DeleteTopicError(topic, _)))
  }

  private def deleteFromSchemaRegistry(topicNames: List[String]): F[Either[DeleteTopicErrorList, Unit]] = {
    topicNames.traverse { topic =>
      val maybeVersion = getVersionFromSchemaRegistry(topic)
      maybeVersion.flatMap(_.traverse(versions => versions.traverse(deleteVersionFromSchemaRegistry(_,topic))))
    }
  }

  def deleteTopic(topicNames: List[String]): F[Either[DeleteTopicErrorList, Unit]] = {
    kafkaClient.deleteTopics(topicNames).flatMap{
      case Right(value) =>{
        deleteFromSchemaRegistry(topicNames) match {
          case Left(errorList) => return Left(errorList)
          case Right(_) => return Right(Unit)
        }
      }
      case Left(value) =>
    }


//    {
//      case Success(maybeSuccess) => {
//        case Success(Right) => {
//          // delete all from schema registry
//
//        }
//        case Success(Left) => {
//          // Either partial success or full failure
//          maybeSuccess.left.map{ topicErrors =>
//            val erroredTopicNames = topicErrors.errors.map(topicError => topicError.topicName).toList
//            if (erroredTopicNames.length == topicNames.length) {
//              // No success
//              return Left(topicErrors)
//            } else {
//              // partial success
//              val partialSuccess = topicNames.filter(topic => !erroredTopicNames.contains(topic))
//              deleteFromSchemaRegistry(partialSuccess) match {
//                case Left(value) => topicErrors += value
//              }
//              return Left(topicErrors)
//            }
//          }
//        }
//        case Failure(e) => {
//          return Left(DeleteTopicErrorList(topicNames.map(topic => DeleteTopicError(topic, e)).toNonEmptyList))
//        }
//      }
//      case Failure(e) =>  {
//        return Left(DeleteTopicErrorList(topicNames.map(topic => DeleteTopicError(topic, e)).toNonEmptyList))
//      }
//    }
  }
}
