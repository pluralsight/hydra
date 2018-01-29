package hydra.core.connect

import akka.actor.{ActorSystem, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import hydra.core.Settings
import hydra.core.ingest.{HydraRequest, IngestionReport}
import hydra.core.protocol.{IngestorCompleted, IngestorTimeout, InitiateRequest}
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.duration._

class ConnectorSpec extends TestKit(ActorSystem("hydra",
  config = ConfigFactory.parseString("akka.actor.provider=cluster")
    .withFallback(ConfigFactory.load())))
  with Matchers
  with FlatSpecLike {

  val mediator = DistributedPubSub(system).mediator
  val ingestor = TestProbe("ingestor")
  mediator ! Subscribe(Settings.IngestTopicName, Some("test"), ingestor.ref)
  val listener = TestProbe("listener")
  system.eventStream.subscribe(listener.ref, classOf[HydraConnectIngestError])

  val cfg = ConfigFactory.parseString(
    """
      |unconfirmed-warning-threshold = 10
      |request.timeout = 2s
      |request.metadata {
      |   test = true
      |}
      |hydra-validation = relaxed
      |hydra-ack="replicated"
      |charset = "test"
      |
      """.stripMargin)

  val connector = TestActorRef[Connector](Props(new Connector {
    override val id: String = "test"

    override def config: Config = cfg
  }))

  "A Connector" should "be configured properly" in {
    val cs = connector.underlyingActor.settings
    val settings = new ConnectorSettings(cfg, system)
    settings.requestTimeout shouldBe cs.requestTimeout
    settings.ackStrategy shouldBe cs.ackStrategy
    settings.validationStrategy shouldBe cs.validationStrategy
    settings.charset shouldBe cs.charset
    settings.metadata shouldBe cs.metadata
  }

  it should "publish errors to the stream" in {
    connector ! IngestionReport("123", Map("test" -> IngestorTimeout), 408)
    listener.expectMsgPF() {
      case HydraConnectIngestError(id, source, statusCode, msg) =>
        statusCode shouldBe 408
        id shouldBe "test"
        source shouldBe "test"
        msg shouldBe "Request Timeout"
    }
  }

  it should "silently accept 200s" in {
    connector ! IngestionReport("123", Map("test" -> IngestorCompleted), 200)
    listener.expectNoMessage(1.second)
  }

  it should "respond to lifecycle methods" in {
    val probe = TestProbe()
    val connector = system.actorOf(Props(new Connector {
      override val id: String = "test"

      override def config: Config = cfg

      override def init() = probe.ref ! "init"

      override def stop() = probe.ref ! "stop"
    }))

    probe.expectMsg("init")
    system.stop(connector)
    probe.expectMsg("stop")
  }

  it should "publish requests" in {
    val connector = system.actorOf(Props(new Connector {
      override val id: String = "test"

      override def config: Config = cfg
    }), "test_connector")
    val req = HydraRequest("123", "test")
    connector ! req
    ingestor.expectMsg(InitiateRequest(req, 2 seconds))
  }
}
