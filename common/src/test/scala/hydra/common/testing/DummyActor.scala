package hydra.common.testing

import akka.actor.Actor
import hydra.common.config.ActorConfigSupport
import hydra.common.logging.ActorLoggingAdapter

/**
  * Created by alexsilva on 3/6/17.
  */
class DummyActor
    extends Actor
    with ActorConfigSupport
    with ActorLoggingAdapter {

  override def receive: Receive = {
    case msg =>
      log.info(msg.toString)
      sender ! msg
  }
}
