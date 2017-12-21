package hydra.ingest.cluster

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.pubsub.DistributedPubSub
import hydra.core.ingest.HydraRequest

/**
  * Publishes Hydra requests to the cluster using the topic name supplied.
  *
  * The message is delivered only once via the supplied `routingLogic`
  * (default random) to one ingest actor within each subscribing group.
  *
  * @param topic
  */
class HydraRequestPublisher(topic: String) extends Actor with ActorLogging {

  import akka.cluster.pubsub.DistributedPubSubMediator.Publish

  val mediator = DistributedPubSub(context.system).mediator

  log.debug(s"Started HydraRequestPublisher for topic `$topic` at ${self.path}")

  def receive = {
    case request: HydraRequest ⇒
      mediator ! Publish(topic, request, true)
  }
}

object HydraRequestPublisher {
  val TopicName = "ingest"
  val GroupName = "hydra-ingest"

  def props(topic: String): Props = Props(classOf[HydraRequestPublisher], TopicName)
}
