package hydra.core.transport

import akka.actor.Actor
import hydra.common.config.ActorConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.akka.ComposingReceive
import hydra.core.protocol._

/**
  * Created by alexsilva on 12/1/15.
  */

trait Transport extends Actor with ActorConfigSupport with LoggingAdapter with ComposingReceive {

  override val baseReceive: Receive = {
    case Produce(_) => log.info(s"Produce message was not handled by ${thisActorName}.")

    case RecordProduced => log.info(s"$thisActorName: Record produced.")

    case RecordNotProduced(_, error) => log.error(s"$thisActorName: $error")
  }

  def transport(next: Receive) = compose(next)
}
