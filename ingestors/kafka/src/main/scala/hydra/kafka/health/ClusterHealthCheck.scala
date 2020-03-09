package hydra.kafka.health

import akka.actor.Actor
import hydra.core.bootstrap.{HealthInfo, HealthState}
import hydra.kafka.health.ClusterHealthCheck.{CheckHealth, GetHealth}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

/**
  * Created by alexsilva on 10/1/16.
  */
class ClusterHealthCheck(
    name: String,
    checkHealth: () => Future[HealthInfo],
    interval: FiniteDuration
) extends Actor {

  implicit val ec = context.dispatcher

  @volatile
  private[health] var currentHealth = HealthInfo(name, details = "")

  override def preStart(): Unit = {
    context.system.scheduler.scheduleAtFixedRate(
      interval,
      interval,
      self,
      CheckHealth
    )
  }

  private def maybePublish(newHealth: HealthInfo): Unit = {
    if (newHealth.state != currentHealth.state || currentHealth.state == HealthState.CRITICAL) {
      context.system.eventStream.publish(newHealth)
    }
    currentHealth = newHealth
  }

  override def receive: Receive = {
    case GetHealth => sender ! currentHealth
    case CheckHealth =>
      checkHealth.apply() onComplete {
        case Success(health) => maybePublish(health)
        case Failure(ex) =>
          maybePublish(
            HealthInfo(
              name,
              details = ex.getMessage,
              state = HealthState.CRITICAL
            )
          )
      }
  }
}

object ClusterHealthCheck {
  case object CheckHealth

  case object GetHealth
}
