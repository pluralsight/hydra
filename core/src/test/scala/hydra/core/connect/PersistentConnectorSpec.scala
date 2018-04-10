package hydra.core.connect

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.persistence.AtLeastOnceDelivery.{ UnconfirmedDelivery, UnconfirmedWarning }
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import com.typesafe.config.{ Config, ConfigFactory }
import hydra.core.Settings
import hydra.core.connect.PersistentConnector.{ GetUnconfirmedCount, UnconfirmedCount }
import hydra.core.ingest.{ HydraRequest, IngestionReport }
import hydra.core.protocol.{ IngestorCompleted, IngestorTimeout, InitiateRequest, InvalidRequest }
import org.scalatest.concurrent.Eventually
import org.scalatest.{ BeforeAndAfterAll, FlatSpecLike, Matchers }

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class PersistentConnectorSpec extends TestKit(ActorSystem(
  "hydra",
  config = ConfigFactory.parseString("""akka.actor.provider=cluster
akka.remote.artery.canonical.port=0""")
    .withFallback(ConfigFactory.load())))
  with Matchers
  with FlatSpecLike
  with Eventually
  with ImplicitSender
  with BeforeAndAfterAll {

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(2000 millis),
    interval = scaled(100 millis))

  val mediator = DistributedPubSub(system).mediator
  val ingestorProbe = TestProbe("ingestor")
  mediator ! Subscribe(Settings.HydraSettings.IngestTopicName, Some("test"), ingestorProbe.ref)

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true)

  def connectorRef(name: String): ActorRef = system.actorOf(Props(new PersistentConnector {
    override val id: String = name

    override def config: Config = ConfigFactory.empty()
  }), name)

  "A PersistentConnector" should "persist HydraRequests" in {
    val connector = connectorRef("persist")
    connector ! HydraRequest("123", "test")
    ingestorProbe.expectMsgPF() {
      case InitiateRequest(r, _, _) =>
        r.correlationId should not be "123" //should be provided by akka now
    }

    connector ! IngestionReport("123", Map.empty, 200)
    system.stop(connector)
  }

  it should "publish UnconfirmedWarning messages to the event stream" in {
    val connector = connectorRef("tester")
    val listener = TestProbe("listener1")
    system.eventStream.subscribe(listener.ref, classOf[HydraConnectIngestError])
    val req = HydraRequest("123", "test")
    var msgs = new ListBuffer[UnconfirmedDelivery]()
    for (i <- 1 to 15) msgs += UnconfirmedDelivery(i, ingestorProbe.ref.path, req)
    connector ! UnconfirmedWarning(msgs.toList)
    listener.expectMsgPF() {
      case HydraConnectIngestError(source, connectorId, statusCode, _, _) =>
        source shouldBe "tester"
        connectorId shouldBe "tester"
        statusCode shouldBe 503
    }
    system.eventStream.unsubscribe(listener.ref)
    system.stop(connector)
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
      case HydraConnectIngestError(source, connectorId, statusCode, _, _) =>
        source shouldBe "unconfirmed-test"
        connectorId shouldBe "unconfirmed-test"
        statusCode shouldBe 503
    }
    system.eventStream.unsubscribe(listener.ref)
    system.stop(c)
  }

  it should "confirm delivery" in {
    val testConnector = system.actorOf(Props(new PersistentConnector {
      override val id: String = "test-confirm"

      override def config: Config = ConfigFactory.empty()
    }), "tc")

    testConnector ! HydraRequest("1234", "test")

    testConnector ! GetUnconfirmedCount

    eventually {
      expectMsg(UnconfirmedCount(1))
      testConnector ! GetUnconfirmedCount
    }
    //we use one here because we know this will be the persistence delivery id.
    testConnector ! IngestionReport("1", Map("test" -> IngestorCompleted), 200)
    testConnector ! GetUnconfirmedCount
    eventually {
      expectMsg(UnconfirmedCount(0))
      testConnector ! GetUnconfirmedCount
    }
    system.stop(testConnector)
  }

  it should "should confirm delivery on client error" in {
    val testConnector = system.actorOf(Props(new PersistentConnector {
      override val id: String = "test-confirm-client-error"

      override def config: Config = ConfigFactory.empty()
    }), "tc")

    testConnector ! HydraRequest("1234", "test")

    testConnector ! GetUnconfirmedCount

    eventually {
      expectMsg(UnconfirmedCount(1))
    }
    //we use one here because we know this will be the persistence delivery id.
    testConnector ! IngestionReport("1", Map("test" -> new InvalidRequest("Bad message")), 400)
    testConnector ! GetUnconfirmedCount
    eventually {
      expectMsg(UnconfirmedCount(0))
    }
    system.stop(testConnector)
  }

  it should "publish errors to the stream" in {
    val testConnector = system.actorOf(Props(new PersistentConnector {
      override val id: String = "publish"

      override def config: Config = ConfigFactory.empty()
    }), "publish")

    val listener = TestProbe("listener3")
    system.eventStream.subscribe(listener.ref, classOf[HydraConnectIngestError])
    testConnector ! IngestionReport("123", Map("test" -> IngestorTimeout), 408)
    listener.expectMsgPF() {
      case HydraConnectIngestError(id, source, statusCode, msg, _) =>
        statusCode shouldBe 408
        id shouldBe "publish"
        source shouldBe "test"
        msg shouldBe "Request Timeout"
    }
    system.eventStream.unsubscribe(listener.ref)
    system.stop(testConnector)
  }
}
