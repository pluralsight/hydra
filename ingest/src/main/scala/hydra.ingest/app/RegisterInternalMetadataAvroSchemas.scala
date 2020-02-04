package hydra.ingest.app

import cats.effect.{IO, Resource, Timer}
import hydra.avro.registry.{KeyValueSchemaRegistrar, SchemaRegistry}
import hydra.kafka.config.KafkaConfigSupport.applicationConfig
import org.apache.avro.Schema
import retry.RetryPolicies.{exponentialBackoff, limitRetries}
import retry.syntax.all._
import retry.{RetryDetails, RetryPolicy, _}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging

import concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.io.Source

class RegisterInternalMetadataAvroSchemas extends LazyLogging {

  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  private val policy: RetryPolicy[IO] = limitRetries[IO](2) |+| exponentialBackoff[IO](500.milliseconds)
  private val onFailure: (Throwable, RetryDetails) => IO[Unit] = (error, retryDetails) => IO {
    logger.info(s"Retrying due to failure: $error. RetryDetails: $retryDetails")
  }

  val valueSchema = new Schema.Parser().parse(Source.fromResource("HydraMetadataTopicV2.avsc").mkString)
  val schemaRegistryUrl: String = applicationConfig.getString("schema.registry.url")
  for {
    registry <- SchemaRegistry.live[IO](schemaRegistryUrl, maxCacheSize = 10)
    valueSchemaResult <- registry.registerSchema(s"${valueSchema.getFullName}-value", valueSchema).retryingOnAllErrors(policy, onFailure)
  } yield ()
}
