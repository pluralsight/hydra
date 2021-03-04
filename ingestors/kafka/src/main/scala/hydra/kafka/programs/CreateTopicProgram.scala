package hydra.kafka.programs

import java.time.Instant

import cats.effect.{Bracket, ExitCase, Resource, Sync}
import cats.syntax.all._
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.{IncompatibleSchemaException, SchemaVersion}
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model.{Schemas, TopicMetadataV2, TopicMetadataV2Key, TopicMetadataV2Request}
import hydra.kafka.programs.CreateTopicProgram.{IncompatibleKeyAndValueFieldNames, KeyAndValueMismatch}
import hydra.kafka.util.KafkaUtils.TopicDetails
import io.chrisdavenport.log4cats.Logger
import org.apache.avro.Schema
import retry.syntax.all._
import retry.{RetryDetails, RetryPolicy, _}

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

final class CreateTopicProgram[F[_]: Bracket[*[_], Throwable]: Sleep: Logger](
                                                                               schemaRegistry: SchemaRegistry[F],
                                                                               kafkaAdmin: KafkaAdminAlgebra[F],
                                                                               kafkaClient: KafkaClientAlgebra[F],
                                                                               retryPolicy: RetryPolicy[F],
                                                                               v2MetadataTopicName: Subject,
                                                                               metadataAlgebra: MetadataAlgebra[F]
) {

  private def onFailure(resourceTried: String): (Throwable, RetryDetails) => F[Unit] = {
    (error, retryDetails) =>
      Logger[F].info(
        s"Retrying due to failure in $resourceTried: $error. RetryDetails: $retryDetails"
      )
  }

  private def registerSchema(
      subject: Subject,
      schema: Schema,
      isKey: Boolean
  ): Resource[F, Unit] = {
    val suffixedSubject = subject.value + (if (isKey) "-key" else "-value")
    val registerSchema: F[Option[SchemaVersion]] = {
      schemaRegistry
        .getVersion(suffixedSubject, schema)
        .attempt
        .map(_.toOption)
        .flatMap { previousSchemaVersion =>
          schemaRegistry.registerSchema(suffixedSubject, schema) *>
            schemaRegistry.getVersion(suffixedSubject, schema).map {
              newSchemaVersion =>
                if (previousSchemaVersion.contains(newSchemaVersion)) None
                else Some(newSchemaVersion)
            }
        }
    }.retryingOnAllErrors(retryPolicy, onFailure("RegisterSchema"))
    Resource
      .makeCase(registerSchema)((newVersionMaybe, exitCase) =>
        (exitCase, newVersionMaybe) match {
          case (ExitCase.Error(_), Some(newVersion)) =>
            schemaRegistry.deleteSchemaOfVersion(suffixedSubject, newVersion)
          case _ => Bracket[F, Throwable].unit
        }
      )
      .void
  }

  private[programs] def registerSchemas(
      subject: Subject,
      keySchema: Schema,
      valueSchema: Schema
  ): Resource[F, Unit] = {
    registerSchema(subject, keySchema, isKey = true) *> registerSchema(
      subject,
      valueSchema,
      isKey = false
    )
  }

  private[programs] def createTopicResource(
      subject: Subject,
      topicDetails: TopicDetails
  ): Resource[F, Unit] = {
    val createTopic: F[Option[Subject]] =
      kafkaAdmin.describeTopic(subject.value).flatMap {
        case Some(_) => Bracket[F, Throwable].pure(None)
        case None =>
          kafkaAdmin
            .createTopic(subject.value, topicDetails)
            .retryingOnAllErrors(retryPolicy, onFailure("CreateTopicResource")) *> Bracket[
            F,
            Throwable
          ].pure(Some(subject))
      }
    Resource
      .makeCase(createTopic)({
        case (Some(_), ExitCase.Error(_)) =>
          kafkaAdmin.deleteTopic(subject.value)
        case _ => Bracket[F, Throwable].unit
      })
      .void
  }

  def publishMetadata(
      topicName: Subject,
      createTopicRequest: TopicMetadataV2Request,
  ): F[Unit] = {
    for {
      metadata <- metadataAlgebra.getMetadataFor(topicName)
      createdDate = metadata.map(_.value.createdDate).getOrElse(createTopicRequest.createdDate)
      deprecatedDate = metadata.map(_.value.deprecatedDate).getOrElse(createTopicRequest.deprecatedDate) match {
        case Some(date) =>
          Some(date)
        case None =>
          if(createTopicRequest.deprecated) {
            Some(Instant.now)
          } else {
            None
          }
      }
      message = (TopicMetadataV2Key(topicName), createTopicRequest.copy(createdDate = createdDate, deprecatedDate = deprecatedDate).toValue)
      records <- TopicMetadataV2.encode[F](message._1, Some(message._2), None)
      _ <- kafkaClient
        .publishMessage(records, v2MetadataTopicName.value)
        .rethrow
    } yield ()
  }

  private def validateKeyAndValueSchemas(schemas: Schemas, subject: Subject): Resource[F, Unit] = {
    val containsMismatches: F[Unit] = kafkaAdmin.describeTopic(subject.value).flatMap {
      case Some(_) => Bracket[F, Throwable].unit
      case None =>
        val keyFields = schemas.key.getFields.asScala.toList
        val valueFields = schemas.value.getFields.asScala.toList
        val mismatches = keyFields.flatMap{ k =>
          valueFields.flatMap { v =>
            if (k.name() == v.name() && !k.schema().equals(v.schema())) {
              Some(KeyAndValueMismatch(k.name(), k.schema(), v.schema()))
            } else {
              None
            }
          }
        }
        if (mismatches.isEmpty) ().pure else Bracket[F, Throwable].raiseError(IncompatibleKeyAndValueFieldNames(mismatches))
    }
    Resource.liftF(containsMismatches)
  }

  def createTopic(
      topicName: Subject,
      createTopicRequest: TopicMetadataV2Request,
      defaultTopicDetails: TopicDetails
  ): F[Unit] = {
    val td = createTopicRequest.numPartitions
      .map(numP => defaultTopicDetails.copy(numPartitions = numP.value)).getOrElse(defaultTopicDetails)
    if(createTopicRequest.schemas.key.getFields.size() <= 0) {
      Bracket[F, Throwable].raiseError(IncompatibleSchemaException("Must include Fields in Key"))
    } else {
      (for {
        _ <- validateKeyAndValueSchemas(createTopicRequest.schemas, topicName)
        _ <- registerSchemas(
          topicName,
          createTopicRequest.schemas.key,
          createTopicRequest.schemas.value
        )
        _ <- createTopicResource(topicName, td)
        _ <- Resource.liftF(publishMetadata(topicName, createTopicRequest))
      } yield ()).use(_ => Bracket[F, Throwable].unit)
    }
  }
}

object CreateTopicProgram {
  final case class KeyAndValueMismatch(fieldName: String, keyFieldSchema: Schema, valueFieldSchema: Schema)
  final case class IncompatibleKeyAndValueFieldNames(errors: List[KeyAndValueMismatch]) extends
    RuntimeException(
      (List("Fields with same names in key and value schemas must have same type:", "Field Name\tKey Schema\tValue Schema") ++ errors.map(e => s"${e.fieldName}\t${e.keyFieldSchema.toString}\t${e.valueFieldSchema}")).mkString("\n")
    )
}
