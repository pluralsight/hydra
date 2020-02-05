package hydra.core.bootstrap

import cats.effect.{IO, Sync, Timer}
import hydra.avro.registry.SchemaRegistry
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.scalatest.{AsyncFlatSpec, Matchers, WordSpec}

class RegisterInternalMetadataAvroSchemasSpec extends WordSpec with Matchers {
  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  implicit val timer: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

  "RegisterInternalSpec" must {
    "register the two avro schemas" in {
      val schemaRegistryIO = SchemaRegistry.test[IO]
      val keyResource = "HydraMetadataTopicKeyV2.avsc"
      val valueResource = "HydraMetadataTopicValueV2.avsc"

      (for {
        schemaRegistry <- schemaRegistryIO
        registerInternalMetadata <- RegisterInternalMetadataAvroSchemas.make[IO](keyResource, valueResource, schemaRegistry)
        _ = registerInternalMetadata.createSchemas().unsafeRunSync()
      } yield succeed).unsafeRunSync()
    }

    "fail to register the two avro schemas" in {
      val schemaRegistryIO = SchemaRegistry.test[IO]
      val keyResource = "Not a Real Avro File"
      val valueResource = "HydraMetadataTopicValueV2.avsc"

      (for {
        schemaRegistry <- schemaRegistryIO
        register <- RegisterInternalMetadataAvroSchemas.make[IO](keyResource, valueResource, schemaRegistry)
        _ <- register.createSchemas()
      } yield ()).attempt.map {
        case Left(_) => succeed
        case Right(_) => fail
      }.unsafeRunSync()
    }

  }

}
