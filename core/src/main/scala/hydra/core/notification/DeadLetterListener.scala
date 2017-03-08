package hydra.core.notification


import akka.actor.{Actor, DeadLetter}
import configs.syntax._

/**
  * Created by alexsilva on 3/8/17.
  */
class DeadLetterListener extends Actor with NotificationSupport {

  lazy val deadLetterDestination = applicationConfig.get[String]("events.deadletter.destination")
    .valueOrElse("__deadletters")

  def receive = {
    case d: DeadLetter => observers ! Publish(deadLetterDestination, d)
  }
}

