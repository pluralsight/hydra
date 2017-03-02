package hydra.core.ingest

import akka.actor.Actor
import akka.event.LoggingReceive

/**
 * Created by alexsilva on 3/3/16.
 */
trait IngestionFlow {
  this: Actor =>

  var receivers: Receive = Actor.emptyBehavior

  def ingest(next: Actor.Receive) = {
    receivers = next orElse receivers
  }

  override def receive = LoggingReceive {
    receivers
  }
}
