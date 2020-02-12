package hydra.core.bootstrap

import cats.MonadError
import cats.data.Validated
import cats.effect.Sync
import cats.implicits._
import hydra.avro.registry.{KeyValueSchemaRegistrar, SchemaRegistry}
import io.chrisdavenport.log4cats.Logger
import org.apache.avro.{Schema, SchemaParseException}
import retry.RetryPolicies.{exponentialBackoff, limitRetries}
import retry.syntax.all._
import retry.{RetryDetails, RetryPolicy, _}

import scala.concurrent.duration._
import scala.io.Source

class RegisterMetadataAvroSchemas[F[_]: Sync: Sleep: Logger](
                                                              keyAvroString: String,
                                                              valueAvroString: String,
                                                              schemaRegistrar: KeyValueSchemaRegistrar[F]) {
  def createSchemas(): F[Unit] = {
    val policy: RetryPolicy[F] = limitRetries[F](2) |+| exponentialBackoff[F](500.milliseconds)
    val onFailure: (Throwable, RetryDetails) => F[Unit] = { (error, retryDetails) =>
      Logger[F].info(s"Retrying due to failure: $error. RetryDetails: $retryDetails")
    }

    val parser = parseSchemasF(keyAvroString, valueAvroString)


    for {
      schemas <- parser
      (keySchema, valueSchema) = schemas
      _ <- schemaRegistrar.registerSchemas(keySchema.getFullName, keySchema, valueSchema).use(_ => Sync[F].pure(())).retryingOnAllErrors(policy, onFailure)
    } yield ()
  }

  private def parseSchemasF(keyAvroString: String, valueAvroString: String): F[(Schema, Schema)] = {
    val keyValidated = Validated.catchNonFatal(new Schema.Parser().parse(keyAvroString)).toValidatedNel
    val valueValidated = Validated.catchNonFatal(new Schema.Parser().parse(valueAvroString)).toValidatedNel
    (keyValidated, valueValidated).mapN((_,_)) match {
      case Validated.Valid(schemas) => Sync[F].pure(schemas)
      case Validated.Invalid(e) => Sync[F].raiseError(new SchemaParseException(e.map(_.getMessage).toList.mkString(" ")))
    }
  }
}

object RegisterMetadataAvroSchemas {
  def make[F[_]: Sync: Sleep: Logger]
      (keyAvroString: String, valueAvroString: String, schemaRegistry: SchemaRegistry[F]): F[RegisterMetadataAvroSchemas[F]] = {
    KeyValueSchemaRegistrar.make[F](schemaRegistry).map { registrar =>
      new RegisterMetadataAvroSchemas(keyAvroString, valueAvroString, registrar)
    }
  }
}
