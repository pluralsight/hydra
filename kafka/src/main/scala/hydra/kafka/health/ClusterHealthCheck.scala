package hydra.kafka.health

import akka.actor.Actor
import com.github.vonnagy.service.container.health._
import com.typesafe.config.{Config, ConfigFactory}
import hydra.kafka.config.KafkaConfigSupport

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

/**
  * Created by alexsilva on 10/1/16.
  */
trait ClusterHealthCheck extends KafkaConfigSupport with RegisteredHealthCheckActor {

  this: Actor =>

  import scala.collection.JavaConverters._

  implicit val ec = context.dispatcher

  def interval: FiniteDuration

  def name: String

  @volatile
  private[health] var currentHealth = HealthInfo(name, details = "")

  override def preStart(): Unit = {
    context.system.scheduler.schedule(interval, interval, self, CheckHealth)
  }

  private def maybePublish(newHealth: HealthInfo): Unit = {
    if (newHealth.state != currentHealth.state || currentHealth.state == HealthState.CRITICAL) {
      context.system.eventStream.publish(newHealth)
    }
    currentHealth = newHealth
  }

  override def receive: Receive = {
    case GetHealth => sender ! currentHealth
    case CheckHealth => checkHealth() onComplete {
      case Success(health) => maybePublish(health)
      case Failure(ex) =>
        maybePublish(HealthInfo(name, details = ex.getMessage, state = HealthState.CRITICAL))
    }
  }

  def checkHealth(): Future[HealthInfo]

  protected lazy val producerConfig: Config =
    ConfigFactory.parseMap(Map(
      "metadata.fetch.timeout.ms" -> (interval.toMillis / 2).toString,
      "client.id" -> "hydra.health.check").asJava).withFallback(kafkaProducerFormats("string"))


  protected lazy val consumerConfig: Config =
    ConfigFactory.parseMap(Map(
      "metadata.fetch.timeout.ms" -> (interval.toMillis / 2).toString,
      "client.id" -> "hydra.health.check").asJava).withFallback(kafkaConsumerFormats("string"))
}



