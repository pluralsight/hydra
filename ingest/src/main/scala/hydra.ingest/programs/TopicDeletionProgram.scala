package hydra.ingest.programs

import cats.MonadError
import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.Concurrent
import cats.implicits._

import java.time.Instant
import hydra.avro.registry.SchemaRegistry
import hydra.avro.util.SchemaWrapper
import hydra.ingest.programs.TopicDeletionProgram._
import hydra.kafka.algebras.ConsumerGroupsAlgebra.{Consumer, DetailedConsumerGroup}
import hydra.kafka.algebras.KafkaAdminAlgebra.KafkaDeleteTopicErrorList
import hydra.kafka.algebras.{ConsumerGroupsAlgebra, KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model.{TopicMetadataV2, TopicMetadataV2Key}
import org.apache.kafka.common.TopicPartition
import scalacache.Cache
import scalacache.modes.try_._


final class TopicDeletionProgram[F[_]: MonadError[*[_], Throwable]: Concurrent](kafkaAdmin: KafkaAdminAlgebra[F],
                                                                    kafkaClient: KafkaClientAlgebra[F],
                                                                    v2MetadataTopicName: Subject,
                                                                    v1MetadataTopicName: Subject,
                                                                    schemaClient: SchemaRegistry[F],
                                                                    metadataAlgebra: MetadataAlgebra[F],
                                                                    consumerGroupAlgebra: ConsumerGroupsAlgebra[F],
                                                                    ignoreConsumers: List[String],
                                                                    allowableTopicDeletionTime: Long)
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
  //consumer group should be pulled from prod, consumer group name will work for what you want it to work for

  def hasTopicReceivedRecentRecords(topicName: String): F[(String, List[Boolean])] = {
    val curTime = Instant.now().toEpochMilli
    kafkaAdmin.getLatestOffsets(topicName).flatMap {
      offsetsMap => {
        offsetsMap.toList.filterNot {
          // remove partitions where offset == 0
          _._2.value == 0
        }.traverse {
          case (partition, offset) => {
            val tp = new TopicPartition(partition.topic, partition.partition)
            // get latest published message, compare timestamp to (NOW - configurable time window)
            kafkaClient.streamStringKeyFromGivenPartitionAndOffset(topicName, "dvs.deletion.consumer.group", false,
              List((tp, offset.value - 1))).take(1).map { case (_, _, timestamp) =>
                timestamp.createTime.getOrElse(0: Long) < curTime - allowableTopicDeletionTime
            }.compile.lastOrError
          }
          case _ =>
            throw new Throwable("Unexpected return type from KafkaAdminAlgebra#getLatestOffsets")
        }
      }
    }.map {list => topicName -> list}
  }

  def checkForActivelyPublishedTopics(topicNames: List[String]): F[List[Either[ActivelyPublishedToError, String]]] = {
    topicNames.traverse { name =>
      val topicsResultsF = hasTopicReceivedRecentRecords(name)

      for {
        topicResults <- topicsResultsF
      } yield {
        if(topicResults._2.contains(false)){
          Left(ActivelyPublishedToError(topicResults._1, allowableTopicDeletionTime))
        } else {
          Right(topicResults._1)
        }
      }
    }
  }

  def findDeletableTopics(eitherErrorOrTopicAPTE: List[Either[ActivelyPublishedToError, String]],
                          eitherErrorOrTopicCSEE: List[Either[ConsumersStillExistError, String]]): List[String] = {
    def getOnlyTopics(list: List[Either[DeleteTopicError, String]]): List[String] = {
      list.map {
        case Right(value) => value
        case Left(_) => ""
      }.filterNot(_ == "")
    }

    val goodList1 = getOnlyTopics(eitherErrorOrTopicAPTE)
    val goodList2 = getOnlyTopics(eitherErrorOrTopicCSEE)
    goodList1.filter { goodList2.contains(_) }
  }

  def findNondeletableTopics(eitherErrorOrTopicAPTE: List[Either[ActivelyPublishedToError, String]],
                             eitherErrorOrTopicCSEE: List[Either[ConsumersStillExistError, String]]): ValidatedNel[DeleteTopicError, Unit] = {
      val errorOrTopicCombined: List[Either[DeleteTopicError, String]] = List.concat(eitherErrorOrTopicAPTE, eitherErrorOrTopicCSEE)
      errorOrTopicCombined.map { e =>
        val vnel: Either[DeleteTopicError, Unit] = e match {
          case Right(_) => Right(())
          case Left(value) => Left(value)
        }
        vnel.toValidatedNel
      }.combineAll
  }

  def checkIfTopicExists(topicName: String): F[(String, Boolean)] = {
    kafkaAdmin.describeTopic(topicName).map(_.isDefined).map((topicName, _))
  }

  //consumer group should be pulled from prod, consumer group name will work for what you want it to work for
  def deleteTopics(topicNames: List[String], ignoreConsumerGroups: List[String]): F[ValidatedNel[DeleteTopicError, Unit]] = {

    val topicsF = topicNames.traverse(checkIfTopicExists)

    def topicHasConsumersCheck(topicsThatExist: List[String]): F[List[Either[ConsumersStillExistError, String]]] =
      checkIfTopicStillHasConsumers(topicsThatExist, ignoreConsumerGroups)
    def topicIsActivelyPublishingCheck(topicsThatExist: List[String]): F[List[Either[ActivelyPublishedToError, String]]] =
      checkForActivelyPublishedTopics(topicsThatExist)

    for {
      topics <- topicsF
      (topicsThatExist, nonExistentTopics) = topics.partition(_._2)
      topicHasConsumers <- topicHasConsumersCheck(topicsThatExist.map(_._1))
      topicIsActivelyPublishing <- topicIsActivelyPublishingCheck(topicsThatExist.map(_._1))
      goodTopics = findDeletableTopics(topicIsActivelyPublishing, topicHasConsumers)
      badTopics = findNondeletableTopics(topicIsActivelyPublishing, topicHasConsumers)
      result <- kafkaAdmin.deleteTopics(goodTopics)
      topicsToDeleteSchemaFor = result match {
        case Right(_) => goodTopics
        case Left(error) =>
          val failedTopicNames = error.errors.map(_.topicName).toList.toSet
          goodTopics.toSet.diff(failedTopicNames).toList
      }
      schemaResult <- deleteFromSchemaRegistry(topicsToDeleteSchemaFor)
      metadataResult <- lookupAndDeleteMetadataForTopics(topicsToDeleteSchemaFor)
    } yield {
      val nonExistentTopicErrors: ValidatedNel[DeleteTopicError, Unit]  = nonExistentTopics.map { fakeTopic =>
        val vnel: Either[DeleteTopicError, Unit] = if (fakeTopic._2) {
          Right(())
        } else {
          Left(TopicDoesNotExistError(fakeTopic._1))
        }
        vnel.toValidatedNel
      }.combineAll
      metadataResult.toEither.leftMap(errors => TopicMetadataDeletionErrors(MetadataDeleteTopicErrorList(errors)))
        .toValidatedNel.combine(schemaResult.toEither.leftMap(a => SchemaDeletionErrors(SchemaDeleteTopicErrorList(a)))
        .toValidatedNel.combine(result.leftMap(KafkaDeletionErrors).toValidatedNel
        .combine {
          val b = guavaCache.removeAll().toEither.leftMap(e => CacheDeletionError(e.getMessage)).map(_ => ()).toValidatedNel
          b
        }
        .combine(badTopics)
        .combine(nonExistentTopicErrors)))
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
final case class ActivelyPublishedToError(topic: String, deleteWindow: Long) extends DeleteTopicError
final case class TopicDoesNotExistError(topic: String) extends DeleteTopicError
final case class CacheDeletionError(message: String) extends DeleteTopicError
