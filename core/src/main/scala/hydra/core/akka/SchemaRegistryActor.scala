package hydra.core.akka

import akka.actor.{Actor, Props}
import akka.pattern.{CircuitBreaker, pipe}
import akka.util.Timeout
import com.typesafe.config.Config
import hydra.avro.registry.{ConfluentSchemaRegistry, SchemaRegistryException}
import hydra.avro.resource.{SchemaResource, SchemaResourceLoader}
import hydra.avro.util.SchemaWrapper
import hydra.common.Settings
import hydra.common.logging.LoggingAdapter
import hydra.core.protocol.HydraApplicationError
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.avro.{Schema, SchemaParseException}
import org.apache.kafka.common.PartitionInfo
import scalacache.cachingF

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * This actor serves as an proxy between the handler registry
  * and the application.
  *
  * Created by alexsilva on 12/5/16.
  */
class SchemaRegistryActor(
    config: Config,
    settings: Option[CircuitBreakerSettings]
) extends Actor
    with LoggingAdapter {

  import SchemaRegistryActor._
  import context.dispatcher

  val breakerSettings = settings getOrElse new CircuitBreakerSettings(config)

  private val registryFailure: Try[SchemaRegistryResponse] => Boolean = {
    case Success(_)                                              => false
    case Failure(ex) if ex.isInstanceOf[SchemaRegistryException] => false
    case Failure(_)                                              => true
  }

  private val breaker = CircuitBreaker(
    context.system.scheduler,
    maxFailures = breakerSettings.maxFailures,
    callTimeout = breakerSettings.callTimeout,
    resetTimeout = breakerSettings.resetTimeout
  ).onOpen(notifyOnOpen())

  val registry = ConfluentSchemaRegistry.forConfig(config)

  log.debug(s"Creating new SchemaRegistryActor for ${registry.registryUrl}")

  val loader = new SchemaResourceLoader(
    registry.registryUrl,
    registry.registryClient,
    metadataCheckInterval = Settings.HydraSettings.SchemaMetadataRefreshInterval
  )

  def getSubject(schema: Schema): String = addSchemaSuffix(schema.getFullName)

  private def errorHandler[U](
      schema: String
  ): PartialFunction[Throwable, Future[U]] = {
    case e: SchemaRegistryException
        if e.getCause.isInstanceOf[RestClientException] && isServerError(e) =>
      throw new IllegalArgumentException(
        s"Schema '$schema' doesnt exist. [Schema registry URL: ${registry.registryUrl}]"
      )
    case e: Throwable =>
      throw e
  }

  private def isServerError(e: SchemaRegistryException) =
    e.getCause.asInstanceOf[RestClientException].getErrorCode < 500

  private def fetchSchema(location: String) = {
    for {
      valueSchema <- loader
        .retrieveValueSchema(location)
        .recoverWith(errorHandler(location))
      keySchema <- loader.retrieveKeySchema(location).map(Some(_)).recover {
        case _ => None
      }
    } yield FetchSchemaResponse(valueSchema, keySchema)

  }

  private def fetchSchemas(locations: List[String]) = {
    for {
      valueSchemas <- loader
        .retrieveValueSchemas(locations)
    } yield FetchSchemasResponse(valueSchemas)
  }

  override def receive = {
    case FetchSchemaRequest(location) =>
      breaker.withCircuitBreaker(fetchSchema(location), registryFailure) pipeTo sender

    case FetchSchemasRequest(locations) =>
      breaker.withCircuitBreaker(fetchSchemas(locations), registryFailure) pipeTo sender

    case RegisterSchemaRequest(json: String) =>
      val maybeRegister = tryHandleRegisterSchema(json)
      val registerSchema = Future.fromTry(maybeRegister)
      val registerSchemaResponse: Future[RegisterSchemaResponse] = breaker
        .withCircuitBreaker(registerSchema, registryFailure)
      pipe(registerSchemaResponse) to sender

    case SchemaRegistered(id, version, schemaString) =>
      val schemaResource =
        SchemaResource(id, version, new Schema.Parser().parse(schemaString))
      loader.loadValueSchemaIntoCache(schemaResource) pipeTo sender

    case FetchAllSchemaVersionsRequest(subject: String) =>
      val allVersionsRequest = for {
        resource <- loader.retrieveValueSchema(addSchemaSuffix(subject))
        allVersions <- Future.sequence((1 to resource.version).map {
          versionNumber => loader.retrieveValueSchema(subject, versionNumber)
        })
      } yield FetchAllSchemaVersionsResponse(allVersions)
      breaker.withCircuitBreaker(allVersionsRequest, registryFailure) pipeTo sender

    case FetchSubjectsRequest =>
      val allSubjectsRequest = Future {
        val subjects = registry.registryClient.getAllSubjects.asScala.map {
          subject => removeSchemaSuffix(subject)
        }
        FetchSubjectsResponse(subjects)
      }
      breaker.withCircuitBreaker(allSubjectsRequest, registryFailure) pipeTo sender

    case FetchSchemaMetadataRequest(subject) =>
      val metadataRequest = loader
        .retrieveValueSchema(subject)
        .map(FetchSchemaMetadataResponse(_))
      breaker.withCircuitBreaker(metadataRequest, registryFailure) pipeTo sender

    case FetchSchemaVersionRequest(subject, version) =>
      val metadataRequest = loader
        .retrieveValueSchema(subject, version)
        .map(FetchSchemaVersionResponse(_))
      breaker.withCircuitBreaker(metadataRequest, registryFailure) pipeTo sender
  }

  private def tryHandleRegisterSchema(
      json: String
  ): Try[RegisterSchemaResponse] = {
    val schemaParser = new Schema.Parser()
    Try {
      val schema = schemaParser.parse(json)
      if (!isValidSchemaName(schema.getName)) {
        throw new SchemaParseException(
          "Schema name may only contain letters and numbers."
        )
      }

      SchemaWrapper.from(schema).validate() match {
        case Success(_) => Success
        case Failure(e) => throw e
      }

      val subject = getSubject(schema)
      log.debug(s"Registering schema ${schema.getFullName}: $json")
      val schemaId = registry.registryClient.register(subject, schema)
      val version = registry.registryClient.getVersion(subject, schema)
      RegisterSchemaResponse(SchemaResource(schemaId, version, schema))
    }
  }

  private def notifyOnOpen() = {
    val msg = s"Schema registry at ${registry.registryUrl} is not responding."
    log.error(msg)
    context.system.eventStream
      .publish(HydraApplicationError(new RuntimeException(msg)))
  }
}

class CircuitBreakerSettings(config: Config) {

  import hydra.common.config.ConfigSupport._

  val maxFailures =
    config.getIntOpt("schema-fetcher.max-failures").getOrElse(5)

  val callTimeout = config
    .getDurationOpt("schema-fetcher.call-timeout")
    .getOrElse(5 seconds)

  val resetTimeout = config
    .getDurationOpt("schema-fetcher.reset-timeout")
    .getOrElse(30 seconds)
}

object SchemaRegistryActor {

  def props(
      config: Config,
      settings: Option[CircuitBreakerSettings] = None
  ): Props = Props(classOf[SchemaRegistryActor], config, settings)

  sealed trait SchemaRegistryRequest

  sealed trait SchemaRegistryResponse

  case class FetchSchemaRequest(location: String) extends SchemaRegistryRequest

  case class FetchSchemasRequest(locations: List[String]) extends SchemaRegistryRequest

  case class FetchSchemasResponse(
     valueSchemas: List[(String, Option[SchemaResource])]
  ) extends SchemaRegistryResponse

  case class FetchSchemaResponse(
      schemaResource: SchemaResource,
      keySchemaResource: Option[SchemaResource]
  ) extends SchemaRegistryResponse

  case class FetchSchemaMetadataRequest(subject: String)
      extends SchemaRegistryRequest

  case class FetchSchemaMetadataResponse(schemaResource: SchemaResource)
      extends SchemaRegistryResponse

  case class FetchSubjectsRequest() extends SchemaRegistryRequest

  case class FetchSubjectsResponse(subjects: Iterable[String])
      extends SchemaRegistryResponse

  case class FetchAllSchemaVersionsRequest(subject: String)
      extends SchemaRegistryRequest

  case class FetchAllSchemaVersionsResponse(versions: Iterable[SchemaResource])
      extends SchemaRegistryResponse

  case class FetchSchemaVersionRequest(subject: String, version: Int)
      extends SchemaRegistryRequest

  case class FetchSchemaVersionResponse(schemaResource: SchemaResource)
      extends SchemaRegistryResponse

  case class RegisterSchemaRequest(json: String) extends SchemaRegistryRequest

  case class RegisterSchemaResponse(schemaResource: SchemaResource)
      extends SchemaRegistryResponse

  case class SchemaRegistered(id: Int, version: Int, schemaString: String)

  val SchemaRegisteredTopic =
    "hydra.core.akka.SchemaRegistryActor.SchemaRegistered"

  val validSchemaNameRegex = "^[a-zA-Z0-9]*$".r
  val schemaParser = new Schema.Parser()
  val schemaSuffix = "-value"
  val hasSuffix = ".*-value$".r

  def validateSchemaName(schemaName: String): Try[Boolean] = {
    if (isValidSchemaName(schemaName)) Success(true)
    else
      Failure(
        new SchemaParseException(
          "Schema name may only contain letters and numbers."
        )
      )
  }

  def isValidSchemaName(schemaName: String): Boolean = {
    validSchemaNameRegex.pattern.matcher(schemaName).matches
  }

  def addSchemaSuffix(subject: String): String = subject match {
    case hasSuffix() => subject
    case _           => subject + schemaSuffix
  }

  def removeSchemaSuffix(subject: String): String = subject match {
    case hasSuffix() => subject.dropRight(schemaSuffix.length)
    case _           => subject
  }

}
