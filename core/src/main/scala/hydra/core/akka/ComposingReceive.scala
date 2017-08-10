package hydra.core.akka

import akka.actor.Actor

/**
  * Allows actors to compose their receive messages by allowing
  * subclasses to "override" or listen to only the messages to which
  * they want to react to.
  *
  * Created by alexsilva on 3/18/17.
  */
trait ComposingReceive {
  this: Actor =>

  var composedReceive: Receive = Actor.emptyBehavior

  def baseReceive: Receive

  def compose(next: Actor.Receive) = {
    composedReceive = next orElse baseReceive
  }

  override def receive: Receive = composedReceive

}
