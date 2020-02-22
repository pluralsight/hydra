package hydra.core.bootstrap

import cats.effect.{Bracket, ExitCase, Resource}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.SchemaVersion
import io.chrisdavenport.log4cats.Logger
import org.apache.avro.Schema
import retry.syntax.all._
import retry.{RetryDetails, RetryPolicy, _}

final class CreateTopicProgram[F[_]: Bracket[*, Throwable]: Sleep: Logger](
    schemaRegistry: SchemaRegistry[F],
    retryPolicy: RetryPolicy[F]
) {

  private def registerSchema(
      subject: String,
      schema: Schema,
      isKey: Boolean
  ): Resource[F, Unit] = {
    val onFailure: (Throwable, RetryDetails) => F[Unit] = {
      (error, retryDetails) =>
        Logger[F].info(
          s"Retrying due to failure: $error. RetryDetails: $retryDetails"
        )
    }
    val suffixedSubject = subject + (if (isKey) "-key" else "-value")
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
      subject: String,
      keySchema: Schema,
      valueSchema: Schema
  ): Resource[F, Unit] = {
    registerSchema(subject, keySchema, isKey = true) *> registerSchema(
      subject,
      valueSchema,
      isKey = false
    )
  }

  def createTopic(
      subject: String,
      keySchema: Schema,
      valueSchema: Schema
  ): F[Unit] = {
    for {
      _ <- registerSchemas(subject, keySchema, valueSchema).use(_ =>
        Bracket[F, Throwable].unit
      )
    } yield ()
  }
}
