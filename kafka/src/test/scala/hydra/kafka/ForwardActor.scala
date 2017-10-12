package hydra.kafka

import akka.actor.{Actor, ActorRef}

class ForwardActor(to: ActorRef) extends Actor {
  def receive = {
    case x => to.forward(x)
  }
}