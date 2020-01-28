package hydra.avro.registry

import cats.effect.{ExitCase, IO, Resource}
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.Schema
import cats.implicits._

trait SchemaRegistryFacade {

  def registerSchemas(subject: String, keySchema: Schema, valueSchema: Schema): Resource[IO, Unit]

}

object SchemaRegistryFacade {

  def make(baseUrl: String, identityCacheSize: Int): IO[SchemaRegistryFacade] = {
    IO(apply(new CachedSchemaRegistryClient(baseUrl, identityCacheSize)))
  }

  private[registry] def apply(schemaRegistryClient: SchemaRegistryClient): SchemaRegistryFacade = new SchemaRegistryFacade {

    private def registerSchema(subject: String, schema: Schema, isKey: Boolean): Resource[IO, Unit] = {
      val suffixedSubject = subject + (if (isKey) "-key" else "-value")
      val registerSchema = IO(schemaRegistryClient.register(suffixedSubject, schema))
      Resource.makeCase(registerSchema)((version, exitCase) => exitCase match {
        case ExitCase.Error(_) => IO(schemaRegistryClient.deleteSchemaVersion(subject, version.toString))
        case _ => IO.unit
      }).map(_ => ())
    }

    override def registerSchemas(subject: String, keySchema: Schema, valueSchema: Schema): Resource[IO, Unit] = {
      registerSchema(subject, keySchema, isKey = true) *> registerSchema(subject, valueSchema, isKey = false)
    }
  }

}
