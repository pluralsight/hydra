package hydra.avro.registry

import cats.Monad
import cats.effect.{ExitCase, Resource, Sync}
import cats.implicits._
import org.apache.avro.Schema

trait KeyValueSchemaRegistrar[F[_]] {

  def registerSchemas(subject: String, keySchema: Schema, valueSchema: Schema): Resource[F, Unit]

}

object KeyValueSchemaRegistrar {

  def make[F[_]: Sync](schemaRegistry: SchemaRegistry[F]): F[KeyValueSchemaRegistrar[F]] = {
    Sync[F].delay((apply(schemaRegistry)))
  }

  private[this] def apply[F[_]: Monad](schemaRegistry: SchemaRegistry[F]): KeyValueSchemaRegistrar[F] = new KeyValueSchemaRegistrar[F] {

    private def registerSchema(subject: String, schema: Schema, isKey: Boolean): Resource[F, Unit] = {
      val suffixedSubject = subject + (if (isKey) "-key" else "-value")
      val registerSchema = schemaRegistry.registerSchema(suffixedSubject, schema) *>
        schemaRegistry.getVersion(suffixedSubject, schema)
      Resource.makeCase(registerSchema)((version, exitCase) => exitCase match {
        case ExitCase.Error(_) => schemaRegistry.deleteSchemaOfVersion(suffixedSubject, version)
        case _ => Monad[F].pure(())
      }).map(_ => ())
    }

    override def registerSchemas(subject: String, keySchema: Schema, valueSchema: Schema): Resource[F, Unit] = {
      registerSchema(subject, keySchema, isKey = true) *> registerSchema(subject, valueSchema, isKey = false)
    }
  }

}
