package hydra.core.connect

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.persistence.AtLeastOnceDelivery.{UnconfirmedDelivery, UnconfirmedWarning}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import hydra.core.Settings
import hydra.core.ingest.{HydraRequest, IngestionReport}
import hydra.core.protocol.{IngestorCompleted, IngestorTimeout, InitiateRequest}
import org.scalatest.{FlatSpecLike, Matchers}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class PersistentConnectorSpec extends TestKit(ActorSystem("hydra",
  config = ConfigFactory.parseString("akka.actor.provider=cluster")
    .withFallback(ConfigFactory.load())))
  with Matchers
  with FlatSpecLike {

  val mediator = DistributedPubSub(system).mediator
  val ingestor = TestProbe("ingestor")
  mediator ! Subscribe(Settings.IngestTopicName, Some("test"), ingestor.ref)


  def connectorRef(name: String): ActorRef = system.actorOf(Props(new PersistentConnector {
    override val id: String = name

    override def config: Config = ConfigFactory.empty()
  }), name)

  "A PersistentConnector" should "persist HydraRequests" in {
    val connector = connectorRef("persist")
    connector ! HydraRequest("123", "test")
    ingestor.expectMsgPF() {
      case InitiateRequest(r, _, _) =>
        r.correlationId should not be "123" //should be provided by akka now
    }

    connector ! IngestionReport("123", Map.empty, 200) //remove from journal
  }

  it should "publish UnconfirmedWarning messages to the event stream" in {
    val connector = connectorRef("tester")
    val listener = TestProbe("listener1")
    system.eventStream.subscribe(listener.ref, classOf[HydraConnectIngestError])
    val req = HydraRequest("123", "test")
    var msgs = new ListBuffer[UnconfirmedDelivery]()
    for (i <- 1 to 15) msgs += UnconfirmedDelivery(i, ingestor.ref.path, req)
    connector ! UnconfirmedWarning(msgs.toList)
    listener.expectMsgPF() {
      case HydraConnectIngestError(source, connectorId, statusCode, _) =>
        source shouldBe "tester"
        connectorId shouldBe "tester"
        statusCode shouldBe 503
    }
    system.eventStream.unsubscribe(listener.ref)
  }

  it should "trigger UnconfirmedWarning in the absence of ingestion reports" in {
    val c = system.actorOf(Props(new PersistentConnector {
      override val id: String = "unconfirmed-test"

      override def config: Config = ConfigFactory.empty()
    }))

    val listener = TestProbe("listener2")
    system.eventStream.subscribe(listener.ref, classOf[HydraConnectIngestError])
    for (i <- 1 to 55) c ! HydraRequest(i.toString, "test")
    listener.expectMsgPF(10 seconds) {
      case HydraConnectIngestError(source, connectorId, statusCode, _) =>
        source shouldBe "unconfirmed-test"
        connectorId shouldBe "unconfirmed-test"
        statusCode shouldBe 503
    }
    system.eventStream.unsubscribe(listener.ref)
  }

  it should "confirm delivery" in {

    val testConnector = TestActorRef[PersistentConnector](Props(new PersistentConnector {
      override val id: String = "test-confirm"

      override def config: Config = ConfigFactory.empty()
    }), "tc")

    testConnector ! HydraRequest("confirm", "test")
    awaitCond(testConnector.underlyingActor.numberOfUnconfirmed == 1,
      max = 10 seconds, interval = 1 second)
    testConnector ! IngestionReport("confirm", Map("test" -> IngestorCompleted), 200)
    awaitCond(testConnector.underlyingActor.numberOfUnconfirmed == 0)
  }

  it should "publish errors to the stream" in {
    val testConnector = TestActorRef[PersistentConnector](Props(new PersistentConnector {
      override val id: String = "publish"

      override def config: Config = ConfigFactory.empty()
    }), "publish")

    val listener = TestProbe("listener3")
    system.eventStream.subscribe(listener.ref, classOf[HydraConnectIngestError])
    testConnector ! IngestionReport("123", Map("test" -> IngestorTimeout), 408)
    listener.expectMsgPF() {
      case HydraConnectIngestError(id, source, statusCode, msg) =>
        statusCode shouldBe 408
        id shouldBe "publish"
        source shouldBe "test"
        msg shouldBe "Request Timeout"
    }
    system.eventStream.unsubscribe(listener.ref)
  }
}
