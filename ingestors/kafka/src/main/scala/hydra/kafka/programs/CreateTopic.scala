package hydra.kafka.programs

import cats.effect.{Bracket, ExitCase, Resource}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.SchemaVersion
import hydra.core.transport.AckStrategy
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model.{
  TopicMetadataV2Key,
  TopicMetadataV2Request,
  TopicMetadataV2Value
}
import hydra.kafka.producer.AvroKeyRecord
import hydra.kafka.util.KafkaClient
import hydra.kafka.util.KafkaUtils.TopicDetails
import io.chrisdavenport.log4cats.Logger
import org.apache.avro.Schema
import retry.syntax.all._
import retry.{RetryDetails, RetryPolicy, _}

final class CreateTopicProgram[F[_]: Bracket[*[_], Throwable]: Sleep: Logger](
    schemaRegistry: SchemaRegistry[F],
    kafkaClient: KafkaClient[F],
    retryPolicy: RetryPolicy[F],
    v2MetadataTopicName: Subject
) {

  private def registerSchema(
      subject: Subject,
      schema: Schema,
      isKey: Boolean
  ): Resource[F, Unit] = {
    val onFailure: (Throwable, RetryDetails) => F[Unit] = {
      (error, retryDetails) =>
        Logger[F].info(
          s"Retrying due to failure: $error. RetryDetails: $retryDetails"
        )
    }
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
    }.retryingOnAllErrors(retryPolicy, onFailure)
    Resource
      .makeCase(registerSchema)((newVersionMaybe, exitCase) =>
        (exitCase, newVersionMaybe) match {
          case (ExitCase.Error(_), Some(newVersion)) =>
            schemaRegistry.deleteSchemaOfVersion(suffixedSubject, newVersion)
          case _ => Bracket[F, Throwable].unit
        }
      )
      .map(_ => ())
  }

  private def registerSchemas(
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

  private def createTopicResource(
      subject: Subject,
      topicDetails: TopicDetails
  ): Resource[F, Unit] =
    Resource.makeCase(kafkaClient.createTopic(subject.value, topicDetails))(
      (_, exitCase) =>
        exitCase match {
          case ExitCase.Error(_) => kafkaClient.deleteTopic(subject.value)
          case _                 => Bracket[F, Throwable].unit
        }
    )

  private def publishMetadata(
      createTopicRequest: TopicMetadataV2Request
  ): F[Unit] = {
    val (key, value) = createTopicRequest.toKeyAndValue
    val keyRecord = TopicMetadataV2Key.recordFormat.to(key)
    val valueRecord = TopicMetadataV2Value.recordFormat.to(value)
    val record = AvroKeyRecord(
      v2MetadataTopicName.value,
      TopicMetadataV2Key.schema,
      TopicMetadataV2Value.schema,
      keyRecord,
      valueRecord,
      AckStrategy.Replicated
    )
    kafkaClient.publishMessage(record).rethrow
  }

  def createTopic(
      createTopicRequest: TopicMetadataV2Request,
      topicDetails: TopicDetails
  ): F[Unit] = {
    (for {
      _ <- registerSchemas(
        createTopicRequest.subject,
        createTopicRequest.schemas.key,
        createTopicRequest.schemas.value
      )
      _ <- createTopicResource(createTopicRequest.subject, topicDetails)
      _ <- Resource.liftF(publishMetadata(createTopicRequest))
    } yield ()).use(_ => Bracket[F, Throwable].unit)
  }
}