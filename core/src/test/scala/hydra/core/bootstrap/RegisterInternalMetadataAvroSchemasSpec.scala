package hydra.core.bootstrap

import cats.effect.{IO, Sync, Timer}
import hydra.avro.registry.SchemaRegistry
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.scalatest.{AsyncFlatSpec, WordSpec}

class RegisterInternalMetadataAvroSchemasSpec extends WordSpec {
  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  implicit val timer: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

  "RegisterInternalSpec" must {
    "register the two avro schemas" in {
      val schemaRegistry = SchemaRegistry.test[IO]

      val metadataRegistry = RegisterInternalMetadataAvroSchemas.make[IO]()

    }
  }

}
