package hydra.core.akka

import akka.actor.{ Actor, Props }
import akka.pattern.{ CircuitBreaker, pipe }
import com.typesafe.config.Config
import hydra.avro.registry.{ ConfluentSchemaRegistry, SchemaRegistryException }
import hydra.avro.resource.{ SchemaResource, SchemaResourceLoader }
import hydra.common.logging.LoggingAdapter
import hydra.core.akka.SchemaFetchActor.{ FetchSchema, RegisterSchema, SchemaFetchResponse }
import hydra.core.protocol.HydraApplicationError
import org.apache.avro.Schema

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

/**
 * This actor serves as an proxy between the handler registry
 * and the application.
 *
 * Created by alexsilva on 12/5/16.
 */
class SchemaFetchActor(config: Config, settings: Option[CircuitBreakerSettings]) extends Actor
  with LoggingAdapter {

  import context.dispatcher

  val breakerSettings = settings getOrElse new CircuitBreakerSettings(config)

  private val registryFailure: Try[SchemaFetchResponse] => Boolean = {
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

  //val futureSchemaId: Future[RegistryClientResponse] = registryClient.register("subject", "schema")

  override def receive = {
    case FetchSchema(location) =>
      val futureResource = loader.retrieveSchema(location).map(SchemaFetchResponse(_))
      breaker.withCircuitBreaker(futureResource, registryFailure) pipeTo sender
    case RegisterSchema(subject, schema) => {
      val id = registry.registryClient.register(subject, schema)
      breaker.withCircuitBreaker(Future(id)) pipeTo sender
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

object SchemaFetchActor {

  case class FetchSchema(location: String)
  case class RegisterSchema(subject: String, schema: Schema)

  case class SchemaFetchResponse(schema: SchemaResource)

  def props(config: Config, settings: Option[CircuitBreakerSettings] = None): Props = Props(
    classOf[SchemaFetchActor], config, settings)

}
