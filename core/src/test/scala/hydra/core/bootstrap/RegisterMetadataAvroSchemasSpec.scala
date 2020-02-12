package hydra.core.bootstrap

import cats.effect.{IO, Sync, Timer}
import hydra.avro.registry.SchemaRegistry
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.avro.SchemaParseException
import org.scalatest.{AsyncFlatSpec, Matchers, WordSpec}

import scala.io.Source

class RegisterMetadataAvroSchemasSpec extends WordSpec with Matchers {
  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  implicit val timer: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

  "RegisterInternalSpec" must {
    "register the two avro schemas" in {
      val schemaRegistryIO = SchemaRegistry.test[IO]

      val keyAvroString = Source.fromResource("HydraMetadataTopicKeyV2.avsc").mkString
      val valueAvroString = Source.fromResource("HydraMetadataTopicValueV2.avsc").mkString

      (for {
        schemaRegistry <- schemaRegistryIO
        registerInternalMetadata <- RegisterMetadataAvroSchemas.make[IO](keyAvroString, valueAvroString, schemaRegistry)
        _ = registerInternalMetadata.createSchemas().unsafeRunSync()
      } yield succeed).unsafeRunSync()
    }

    "fail to register the two avro schemas" in {
      val schemaRegistryIO = SchemaRegistry.test[IO]
      val keyAvroString = "NotAnAvroSchema"
      val valueAvroString = Source.fromResource("HydraMetadataTopicValueV2.avsc").mkString

      (for {
        schemaRegistry <- schemaRegistryIO
        register <- RegisterMetadataAvroSchemas.make[IO](keyAvroString, valueAvroString, schemaRegistry)
        _ <- register.createSchemas()
      } yield ()).attempt.map {
        case Left(_: SchemaParseException) => succeed
        case Right(_) => fail()
        case _ => fail()
      }.unsafeRunSync()
    }

  }

}
