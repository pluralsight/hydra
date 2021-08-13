package hydra.ingest.programs

import cats.MonadError
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.SchemaVersion
import hydra.kafka.algebras.KafkaAdminAlgebra.KafkaDeleteTopicErrorList
import hydra.kafka.algebras.{ConsumerGroupsAlgebra, KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.implicits._
import hydra.avro.util.SchemaWrapper
import hydra.ingest.programs.TopicDeletionProgram._
import hydra.ingest.services.IngestionFlowV2
import hydra.kafka.algebras.ConsumerGroupsAlgebra.{Consumer, DetailedConsumerGroup}
import hydra.kafka.algebras.KafkaClientAlgebra.PublishError
import hydra.kafka.model.{TopicMetadata, TopicMetadataV2, TopicMetadataV2Key}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import scalacache.Cache
import scalacache.guava.GuavaCache
import scalacache.modes.try_._

final class TopicDeletionProgram[F[_]: MonadError[*[_], Throwable]](kafkaAdmin: KafkaAdminAlgebra[F],
                                                                    kafkaClient: KafkaClientAlgebra[F],
                                                                    v2MetadataTopicName: Subject,
                                                                    v1MetadataTopicName: Subject,
                                                                    schemaClient: SchemaRegistry[F],
                                                                    metadataAlgebra: MetadataAlgebra[F],
                                                                    consumerGroupAlgebra: ConsumerGroupsAlgebra[F],
                                                                    ignoreConsumers: List[String])
                                                                   (implicit guavaCache: Cache[SchemaWrapper]){

  def deleteFromSchemaRegistry(topicNames: List[String]): F[ValidatedNel[SchemaRegistryError, Unit]] = {
    topicNames.flatMap(topic => List(topic + "-key", topic + "-value")).traverse { subject =>
      schemaClient.deleteSchemaSubject(subject).attempt.map {
        _.leftMap(error => SchemaFailToDelete(subject, error)).toValidatedNel
      }
    }.map(_.combineAll)
  }

  private def checkIfTopicStillHasConsumers(topicNames: List[String], ignoreConsumerGroups: List[String]): F[List[Either[ConsumersStillExistError, String]]] = {
    topicNames.traverse { topic =>
      consumerGroupAlgebra.getConsumersForTopic(topic).flatMap { topicConsumers =>
        val fullIgnoreList = ignoreConsumerGroups ++ ignoreConsumers

        val filteredConsumersF: F[List[DetailedConsumerGroup]] =
          topicConsumers.consumers
            .filterNot(consumer => fullIgnoreList.contains(consumer.consumerGroupName))
            .traverse { consumer =>
              consumerGroupAlgebra.getDetailedConsumerInfo(consumer.consumerGroupName)
                .map(listOfInfo => listOfInfo.filter(_.state.getOrElse("Unknown") != "Empty"))
            }.map(_.flatten)


        for {
          filteredConsumers <- filteredConsumersF
        } yield {
          if (filteredConsumers.nonEmpty) {
            Left(ConsumersStillExistError(topic, filteredConsumers.map(detailedConsumer =>
              Consumer(detailedConsumer.consumergroupName, detailedConsumer.lastCommit, detailedConsumer.state))))
          } else {
            Right(topic)
          }
        }
      }
    }
  }

  def deleteTopic(topicNames: List[String], ignoreConsumerGroups: List[String]): F[ValidatedNel[DeleteTopicError, Unit]] = {
    val lastOffsets = kafkaAdmin.getLatestOffsets(topicNames.last).flatMap{ tpo =>
      tpo.map{ case (partition, offset) => {
        val lastConsumed = kafkaClient.streamStringKeyFromGivenPartitionAndOffset(
          topicNames.last, "dvs.deletion.consumer.group",false, partition, offset.value).take(1).compile.map{ record =>
          record._3
        }
      }

      }
    }
    val eitherErrorOrTopic = checkIfTopicStillHasConsumers(topicNames, ignoreConsumerGroups)
    eitherErrorOrTopic.flatMap{ cge =>
      val goodTopics: List[String] = cge.map { e =>
        e match {
          case Right(value) => value
          case Left(_) => ""
        }
      }.filterNot(_ == "")
      val consumerErrors = cge.map { e =>
        val vnel = e match {
          case Right(_) => Right(())
          case Left(value) => Left(value)
        }
        vnel.toValidatedNel
      }.combineAll
      kafkaAdmin.deleteTopics(goodTopics).flatMap { result =>
        val topicsToDeleteSchemaFor = result match {
          case Right(_) => goodTopics
          case Left(error) =>
            val failedTopicNames = error.errors.map(_.topicName).toList.toSet
            goodTopics.toSet.diff(failedTopicNames).toList
        }
        deleteFromSchemaRegistry(topicsToDeleteSchemaFor).flatMap(schemaResult =>
          lookupAndDeleteMetadataForTopics(topicsToDeleteSchemaFor).map(metadataResult =>
            metadataResult.toEither.leftMap(errors => TopicMetadataDeletionErrors(MetadataDeleteTopicErrorList(errors)))
              .toValidatedNel.combine(schemaResult.toEither.leftMap(a => SchemaDeletionErrors(SchemaDeleteTopicErrorList(a)))
              .toValidatedNel.combine(result.leftMap(KafkaDeletionErrors).toValidatedNel)
              .combine{val b = guavaCache.removeAll().toEither.leftMap(e => CacheDeletionError(e.getMessage)).map(_ => ()).toValidatedNel
                b}
            .combine(consumerErrors))))
        }
    }
  }

  private def lookupAndDeleteMetadataForTopics(topicNames: List[String]): F[ValidatedNel[MetadataFailToDelete, Unit]] = {
    topicNames.traverse(topicName => {
      Subject.createValidated(topicName) match {
        case Some(subject) =>
          metadataAlgebra.getMetadataFor(subject).flatMap {
            case Some(_) =>
              deleteV2Metadata(subject).attempt.map {
                _.leftMap(error => MetadataFailToDelete(topicName, v1MetadataTopicName, error)).toValidatedNel
              }
            case other =>
              deleteV1Metadata(topicName).attempt.map {
                _.leftMap(error => MetadataFailToDelete(topicName, v1MetadataTopicName, error)).toValidatedNel
              }
          }
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
          .publishStringKeyMessage((Some(topicName), None, None), v1MetadataTopicName.toString)
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

  final case class MetadataFailToDelete(subject: String, metadataTopic: Subject, cause: Throwable)
    extends Exception(s"Unable to delete $subject from $metadataTopic", cause)

  final case class MetadataDeleteTopicErrorList(errors: NonEmptyList[MetadataFailToDelete])
    extends Exception(s"Topic(s) failed to delete metadata: \n${errors.map(er => s"${er.metadataTopic} - ${er.subject}").toList.mkString("\n")}")
}

sealed abstract class DeleteTopicError extends RuntimeException
final case class KafkaDeletionErrors(kafkaDeleteTopicErrorList: KafkaDeleteTopicErrorList) extends DeleteTopicError
final case class SchemaDeletionErrors(schemaDeleteTopicErrorList: SchemaDeleteTopicErrorList) extends DeleteTopicError
final case class TopicMetadataDeletionErrors(metadataDeleteTopicErrorList: MetadataDeleteTopicErrorList) extends DeleteTopicError
final case class ConsumersStillExistError(topic: String, consumers: List[Consumer]) extends DeleteTopicError
final case class CacheDeletionError(message: String) extends DeleteTopicError
