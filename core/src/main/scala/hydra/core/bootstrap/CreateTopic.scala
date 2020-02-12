package hydra.core.bootstrap

import cats.Monad
import cats.effect.{ExitCase, Resource, Sync}
import cats.implicits._
import hydra.avro.registry.SchemaRegistry
import io.chrisdavenport.log4cats.Logger
import org.apache.avro.Schema
import retry.syntax.all._
import retry.{RetryDetails, RetryPolicy, _}

final class CreateTopic[F[_]: Sync: Sleep: Logger](
                                                    schemaRegistry: SchemaRegistry[F],
                                                    retryPolicy: RetryPolicy[F]) {

  private def registerSchema(subject: String, schema: Schema, isKey: Boolean): Resource[F, Unit] = {
    val onFailure: (Throwable, RetryDetails) => F[Unit] = { (error, retryDetails) =>
      Logger[F].info(s"Retrying due to failure: $error. RetryDetails: $retryDetails")
    }
    val suffixedSubject = subject + (if (isKey) "-key" else "-value")
    val registerSchema = (schemaRegistry.registerSchema(suffixedSubject, schema) *>
      schemaRegistry.getVersion(suffixedSubject, schema)).retryingOnAllErrors(retryPolicy, onFailure)
    Resource.makeCase(registerSchema)((version, exitCase) => exitCase match {
      case ExitCase.Error(_) => schemaRegistry.deleteSchemaOfVersion(suffixedSubject, version)
      case _ => Sync[F].unit
    }).map(_ => ())
  }

  private def registerSchemas(subject: String, keySchema: Schema, valueSchema: Schema): Resource[F, Unit] = {
    registerSchema(subject, keySchema, isKey = true) *> registerSchema(subject, valueSchema, isKey = false)
  }

  def createSchemas(subject: String, keySchema: Schema, valueSchema: Schema): F[Unit] = {
    for {
      _ <- registerSchemas(subject, keySchema, valueSchema).use(_ => Sync[F].unit)
    } yield ()
  }
}