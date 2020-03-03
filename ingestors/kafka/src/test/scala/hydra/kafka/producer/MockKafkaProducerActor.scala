package hydra.kafka.producer

import akka.actor.Actor

/**
  * Created by alexsilva on 2/23/17.
  */
class MockKafkaProducerActor extends Actor {

  override def receive: Receive = {
    case x => println(x)
  }
}
