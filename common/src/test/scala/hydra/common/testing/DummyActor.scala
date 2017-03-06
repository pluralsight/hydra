package hydra.common.testing

import akka.actor.Actor

/**
  * Created by alexsilva on 3/6/17.
  */
class DummyActor extends Actor {
  override def receive: Receive = {
    case msg => sender ! msg
  }
}