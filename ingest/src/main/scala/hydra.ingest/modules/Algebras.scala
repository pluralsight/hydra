package hydra.ingest.modules

import hydra.avro.registry.SchemaRegistry
import hydra.ingest.app.AppConfig.SchemaRegistryConfig
import cats.effect.Sync
import cats.implicits._

final class Algebras[F[_]] private (
    val schemaRegistry: SchemaRegistry[F]
)

object Algebras {

  def make[F[_]: Sync](
      schemaRegistryConfig: SchemaRegistryConfig
  ): F[Algebras[F]] =
    for {
      schemaRegistry <- SchemaRegistry.live[F](
        schemaRegistryConfig.fullUrl,
        schemaRegistryConfig.maxCacheSize
      )
    } yield new Algebras[F](schemaRegistry)
}
