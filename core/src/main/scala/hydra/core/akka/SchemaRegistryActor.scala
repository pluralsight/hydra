package hydra.core.akka

import akka.actor.{Actor, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.pattern.{CircuitBreaker, pipe}
import com.typesafe.config.Config
import hydra.avro.registry.{ConfluentSchemaRegistry, SchemaRegistryException}
import hydra.avro.resource.{SchemaResource, SchemaResourceLoader}
import hydra.common.logging.LoggingAdapter
import hydra.core.protocol.HydraApplicationError
import org.apache.avro.{Schema, SchemaParseException}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * This actor serves as an proxy between the handler registry
  * and the application.
  *
  * Created by alexsilva on 12/5/16.
  */
class SchemaRegistryActor(config: Config, settings: Option[CircuitBreakerSettings]) extends Actor
  with LoggingAdapter {

  import SchemaRegistryActor._
  import context.dispatcher

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

  val loader = new SchemaResourceLoader(registry.registryUrl, registry.registryClient)
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe(SchemaRegisteredTopic, self)

  def getSubject(schema: Schema): String = addSchemaSuffix(schema.getFullName)

  override def receive = {
    case FetchSchemaRequest(location) =>
      val futureResource = loader.retrieveSchema(location).map(resource => FetchSchemaResponse(resource))
      breaker.withCircuitBreaker(futureResource, registryFailure) pipeTo sender

    case RegisterSchemaRequest(json: String) =>
      val tryRegisterSchema = tryHandleRegisterSchema(json)
      val registerSchemaRequest: Future[RegisterSchemaResponse] = breaker
        .withCircuitBreaker(Future.fromTry(tryRegisterSchema), registryFailure)

      registerSchemaRequest.foreach { registerSchemaResponse =>
        val registeredResponse = SchemaRegistered(registerSchemaResponse.schemaResource)
        mediator ! Publish(SchemaRegisteredTopic, registeredResponse)
      }

      pipe(registerSchemaRequest) to sender

    case SchemaRegistered(schemaResource) =>
      loader.loadSchemaIntoCache(schemaResource) pipeTo sender

    case FetchAllSchemaVersionsRequest(subject: String) =>
      val allVersionsRequest = for {
        resource <- loader.retrieveSchema(addSchemaSuffix(subject))
        allVersions <- Future.sequence((1 to resource.version).map { versionNumber =>
          loader.retrieveSchema(subject, versionNumber)
        })
      } yield FetchAllSchemaVersionsResponse(allVersions)

      breaker.withCircuitBreaker(allVersionsRequest, registryFailure) pipeTo sender

    case FetchSubjectsRequest =>
      val allSubjectsRequest = Try {
        val subjects = registry.registryClient.getAllSubjects.asScala.map { subject =>
          removeSchemaSuffix(subject)
        }
        FetchSubjectsResponse(subjects)
      }
      breaker.withCircuitBreaker(Future.fromTry(allSubjectsRequest), registryFailure) pipeTo sender

    case FetchSchemaMetadataRequest(subject) =>
      val metadataRequest = loader.retrieveSchema(subject)
        .map(FetchSchemaMetadataResponse(_))
      breaker.withCircuitBreaker(metadataRequest, registryFailure) pipeTo sender

    case FetchSchemaVersionRequest(subject, version) =>
      val metadataRequest = loader.retrieveSchema(subject, version)
        .map(FetchSchemaVersionResponse(_))
      breaker.withCircuitBreaker(metadataRequest, registryFailure) pipeTo sender
  }

  private def tryHandleRegisterSchema(json: String): Try[RegisterSchemaResponse] = {
    val schemaParser = new Schema.Parser()
    Try {
      val schema = schemaParser.parse(json)
      if (!isValidSchemaName(schema.getName)) {
        throw new SchemaParseException("Schema name may only contain letters and numbers.")
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

  case class FetchSchemaResponse(schemaResource: SchemaResource) extends SchemaRegistryResponse

  case class FetchSchemaMetadataRequest(subject: String) extends SchemaRegistryRequest

  case class FetchSchemaMetadataResponse(schemaResource: SchemaResource) extends SchemaRegistryResponse

  case class FetchSubjectsRequest() extends SchemaRegistryRequest

  case class FetchSubjectsResponse(subjects: Iterable[String]) extends SchemaRegistryResponse

  case class FetchAllSchemaVersionsRequest(subject: String) extends SchemaRegistryRequest

  case class FetchAllSchemaVersionsResponse(versions: Iterable[SchemaResource]) extends SchemaRegistryResponse

  case class FetchSchemaVersionRequest(subject: String, version: Int) extends SchemaRegistryRequest

  case class FetchSchemaVersionResponse(schemaResource: SchemaResource) extends SchemaRegistryResponse

  case class RegisterSchemaRequest(json: String) extends SchemaRegistryRequest

  case class RegisterSchemaResponse(schemaResource: SchemaResource) extends SchemaRegistryResponse

  case class SchemaRegistered(schemaResource: SchemaResource)

  val SchemaRegisteredTopic = "hydra.core.akka.SchemaRegistryActor.SchemaRegistered"

  val validSchemaNameRegex = "^[a-zA-Z0-9]*$".r
  val schemaParser = new Schema.Parser()
  val schemaSuffix = "-value"
  val hasSuffix = ".*-value$".r

  def validateSchemaName(schemaName: String): Try[Boolean] = {
    if (isValidSchemaName(schemaName)) Success(true) else Failure(new SchemaParseException("Schema name may only contain letters and numbers."))
  }

  def isValidSchemaName(schemaName: String): Boolean = {
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
