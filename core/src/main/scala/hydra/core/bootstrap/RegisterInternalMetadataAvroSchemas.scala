package hydra.core.bootstrap

import cats.effect.Sync
import cats.implicits._
import hydra.avro.registry.{KeyValueSchemaRegistrar, SchemaRegistry}
import io.chrisdavenport.log4cats.Logger
import org.apache.avro.Schema
import retry.RetryPolicies.{exponentialBackoff, limitRetries}
import retry.syntax.all._
import retry.{RetryDetails, RetryPolicy, _}

import scala.concurrent.duration._
import scala.io.Source

class RegisterInternalMetadataAvroSchemas[F[_]: Sync: Sleep: Logger](
                                                        keyAvroResource: String,
                                                        valueAvroResource: String,
                                                        schemaRegistrar: KeyValueSchemaRegistrar[F]) {
  def createSchemas(): F[Unit] = {
    val policy: RetryPolicy[F] = limitRetries[F](2) |+| exponentialBackoff[F](500.milliseconds)
    val onFailure: (Throwable, RetryDetails) => F[Unit] = (error, retryDetails) =>
      Logger[F].info(s"Retrying due to failure: $error. RetryDetails: $retryDetails")

    val keySchemaF = Sync[F].delay(new Schema.Parser().parse(Source.fromResource(keyAvroResource).mkString))
    val valueSchemaF = Sync[F].delay(new Schema.Parser().parse(Source.fromResource(valueAvroResource).mkString))

    for {
      keySchema <- keySchemaF
      valueSchema <- valueSchemaF
      _ <- schemaRegistrar.registerSchemas(keySchema.getFullName, keySchema, valueSchema).use(_ => Sync[F].pure(())).retryingOnAllErrors(policy, onFailure)
    } yield (())
  }
}

object RegisterInternalMetadataAvroSchemas {
  def make[F[_]: Sync: Sleep: Logger]
      (keyAvroResource: String, valueAvroResource: String, schemaRegistry: SchemaRegistry[F]): F[RegisterInternalMetadataAvroSchemas[F]] = {
    KeyValueSchemaRegistrar.make[F](schemaRegistry).map { registrar =>
      new RegisterInternalMetadataAvroSchemas(keyAvroResource, valueAvroResource, registrar)
    }
  }
}
