package hydra.ingest.cluster

import akka.actor.ActorSystem
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.core.ingest
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class HydraRequestPublisherSpec extends
  TestKit(ActorSystem("hydra",
    config = ConfigFactory.parseString("akka.actor.provider=cluster")
      .withFallback(ConfigFactory.load()))) with Matchers with FlatSpecLike
  with BeforeAndAfterAll with Eventually {

  val publisher = system.actorOf(HydraRequestPublisher.props(HydraRequestPublisher.TopicName))

  val mediator = DistributedPubSub(system).mediator
  val sub = TestProbe()
  mediator ! Subscribe(HydraRequestPublisher.TopicName, Some(HydraRequestPublisher.GroupName),
    sub.ref)

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system, verifySystemShutdown = true)

  "The HydraRequestPublisher" should "broadcast HydraRequests" in {
    val request = ingest.HydraRequest("1", "payload")
    publisher ! request
    eventually(sub.expectMsg(request))
  }
}