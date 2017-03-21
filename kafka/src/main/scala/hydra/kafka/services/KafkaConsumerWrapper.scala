package hydra.kafka.services

import akka.actor.{Actor, PoisonPill, ReceiveTimeout}
import akka.kafka.{ConsumerSettings, KafkaConsumerActor}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 10/7/16.
  */
class KafkaConsumerWrapper[K, V](settings: ConsumerSettings[K, V]) extends Actor {

  val kafkaConsumer = context.actorOf(KafkaConsumerActor.props(settings))

  context.setReceiveTimeout(30.seconds)

  override def receive = {
    case ReceiveTimeout =>
      kafkaConsumer ! PoisonPill
      context.stop(self)

    case message => kafkaConsumer forward message
  }
}
