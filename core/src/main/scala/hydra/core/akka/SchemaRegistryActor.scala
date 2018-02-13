package hydra.core.akka

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import akka.actor.{Actor, Props}
import akka.pattern.{CircuitBreaker, pipe}
import com.typesafe.config.Config
import hydra.avro.registry.{ConfluentSchemaRegistry, SchemaRegistryException}
import hydra.avro.resource.{SchemaResource, SchemaResourceLoader}
import hydra.common.logging.LoggingAdapter
import hydra.core.protocol.HydraApplicationError
import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import org.apache.avro.{Schema, SchemaParseException}

/**
 * This actor serves as an proxy between the handler registry
 * and the application.
 *
 * Created by alexsilva on 12/5/16.
 */
class SchemaRegistryActor(config: Config, settings: Option[CircuitBreakerSettings]) extends Actor
  with LoggingAdapter {

  import context.dispatcher
  import SchemaRegistryActor._

  val breakerSettings = settings getOrElse new CircuitBreakerSettings(config)

  private val registryFailure: Try[SchemaRegistryResponse] => Boolean = {
    case Success(_) => false
    case Failure(ex) if ex.isInstanceOf[SchemaRegistryException] => false
    case Failure(_) => true
  }

  private val breaker = CircuitBreaker(
    context.system.scheduler,
    maxFailures = breakerSettings.maxFailures,
    callTimeout = breakerSettings.callTimeout,
    resetTimeout = breakerSettings.resetTimeout)
    .onOpen(notifyOnOpen())

  val registry = ConfluentSchemaRegistry.forConfig(config)
  val schemaParser = new Schema.Parser()

  val loader = new SchemaResourceLoader(registry.registryUrl, registry.registryClient)

  def getSubject(schema: Schema): String = addSchemaSuffix(schema.getFullName)

  override def receive = {
    case FetchSchemaRequest(location) =>
      val futureResource = loader.retrieveSchema(location).map(FetchSchemaResponse(_))
      breaker.withCircuitBreaker(futureResource, registryFailure) pipeTo sender

    case RegisterSchemaRequest(json: String) =>
      val metadataRequest = for {
        schema <- Try(schemaParser.parse(json))
        _ <- Try(validateSchemaName(schema.getName()))
        subject <- Try(getSubject(schema))
        schemaId <- Try {
          log.debug(s"Registering schema ${schema.getFullName}: $json")
          registry.registryClient.register(subject, schema)
        }
        registeredSchema <- Try(registry.registryClient.getByID(schemaId))
        version <- Try(registry.registryClient.getVersion(subject, schema))
        metadata <- Try(registry.registryClient.getSchemaMetadata(subject, version))
      } yield RegisterSchemaResponse(registeredSchema.getFullName, metadata.getId, metadata.getVersion, metadata.getSchema)
      breaker.withCircuitBreaker(Future.fromTry(metadataRequest), registryFailure) pipeTo sender

    case FetchAllSchemaVersionsRequest(subject: String) =>
      val allVersionsRequest = for {
        metadata <- Try(registry.registryClient.getLatestSchemaMetadata(addSchemaSuffix(subject)))
        allVersions <- Try {
          (1 to metadata.getVersion).map { versionNumber =>
            registry.registryClient.getSchemaMetadata(addSchemaSuffix(subject), versionNumber)
          }
        }
      } yield FetchAllSchemaVersionsResponse(allVersions)
      breaker.withCircuitBreaker(Future.fromTry(allVersionsRequest), registryFailure) pipeTo sender

    case FetchSubjectsRequest =>
      val allSubjectsRequest = Try {
        val subjects = registry.registryClient.getAllSubjects.asScala.map { subject =>
          removeSchemaSuffix(subject)
        }
        FetchSubjectsResponse(subjects)
      }
      breaker.withCircuitBreaker(Future.fromTry(allSubjectsRequest), registryFailure) pipeTo sender

    case FetchSchemaMetadataRequest(subject) =>
      val metadataRequest = Try(registry.registryClient.getLatestSchemaMetadata(addSchemaSuffix(subject)))
        .map(FetchSchemaMetadataResponse(_))
      breaker.withCircuitBreaker(Future.fromTry(metadataRequest), registryFailure) pipeTo sender

    case FetchSchemaVersionRequest(subject, version) =>
      val metadataRequest = Try(registry.registryClient.getSchemaMetadata(addSchemaSuffix(subject), version))
        .map(FetchSchemaVersionResponse(_))
      breaker.withCircuitBreaker(Future.fromTry(metadataRequest), registryFailure) pipeTo sender
  }

  private def notifyOnOpen() = {
    val msg = s"Schema registry at ${registry.registryUrl} is not responding."
    log.error(msg)
    context.system.eventStream.publish(HydraApplicationError(new RuntimeException(msg)))
  }
}

class CircuitBreakerSettings(config: Config) {

  import configs.syntax._

  val maxFailures = config.get[Int]("schema-fetcher.max-failures").valueOrElse(5)
  val callTimeout = config.get[FiniteDuration]("schema-fetcher.call-timeout")
    .valueOrElse(5 seconds)
  val resetTimeout = config.get[FiniteDuration]("schema-fetcher.reset-timeout")
    .valueOrElse(30 seconds)
}

object SchemaRegistryActor {

  def props(config: Config, settings: Option[CircuitBreakerSettings] = None): Props = Props(
    classOf[SchemaRegistryActor], config, settings)

  sealed trait SchemaRegistryRequest
  sealed trait SchemaRegistryResponse

  case class FetchSchemaRequest(location: String) extends SchemaRegistryRequest
  case class FetchSchemaResponse(schema: SchemaResource) extends SchemaRegistryResponse

  case class FetchSchemaMetadataRequest(subject: String) extends SchemaRegistryRequest
  case class FetchSchemaMetadataResponse(schemaMetadata: SchemaMetadata) extends SchemaRegistryResponse

  case class FetchSubjectsRequest() extends SchemaRegistryRequest
  case class FetchSubjectsResponse(subjects: Iterable[String]) extends SchemaRegistryResponse

  case class FetchAllSchemaVersionsRequest(subject: String) extends SchemaRegistryRequest
  case class FetchAllSchemaVersionsResponse(versions: Iterable[SchemaMetadata]) extends SchemaRegistryResponse

  case class FetchSchemaVersionRequest(subject: String, version: Int) extends SchemaRegistryRequest
  case class FetchSchemaVersionResponse(schemaMetadata: SchemaMetadata) extends SchemaRegistryResponse

  case class RegisterSchemaRequest(json: String) extends SchemaRegistryRequest
  case class RegisterSchemaResponse(name: String, id: Int, version: Int, schema: String) extends SchemaRegistryResponse

  val validSchemaNameRegex = "^[a-zA-Z0-9]*$".r
  val schemaParser = new Schema.Parser()
  val schemaSuffix = "-value"
  val hasSuffix = ".*-value$".r

  def validateSchemaName(schemaName: String): Boolean = {
    if (isValidSchemaName(schemaName)) {
      return true
    } else {
      throw new SchemaParseException("Schema name may only contain letters and numbers.")
    }
  }

  def isValidSchemaName(schemaName: String) = {
    validSchemaNameRegex.pattern.matcher(schemaName).matches
  }

  def addSchemaSuffix(subject: String): String = subject match {
    case hasSuffix() => subject
    case _ => subject + schemaSuffix
  }

  def removeSchemaSuffix(subject: String): String = subject match {
    case hasSuffix() => subject.dropRight(schemaSuffix.length)
    case _ => subject
  }
}
