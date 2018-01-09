package hydra.ingest.services

import akka.actor.{Actor, ActorSystem}
import akka.cluster.pubsub.DistributedPubSub
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import hydra.core.ingest
import hydra.core.ingest.IngestionReport
import hydra.core.protocol._
import hydra.ingest.IngestorInfo
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName, LookupResult}
import hydra.ingest.test.TestRecordFactory
import org.joda.time.DateTime
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.duration._

class IngestionHandlerGatewayClusterSpec extends TestKit(ActorSystem("hydra",
  config = ConfigFactory.parseString("akka.actor.provider=cluster")
    .withFallback(ConfigFactory.load()))) with Matchers with FlatSpecLike
  with BeforeAndAfterAll with Eventually with ImplicitSender {

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true,
    duration = 10 seconds)

  val mediator = DistributedPubSub(system).mediator

  val ingestor = TestActorRef(new Actor {
    override def receive = {
      case Publish(_) => sender ! Join
      case Validate(r) => sender ! ValidRequest(TestRecordFactory.build(r).get)
      case Ingest(r, _) => sender ! IngestorCompleted
    }
  }, "test_ingestor")

  val ingestorInfo = IngestorInfo("test_ingestor", "test", ingestor.path, DateTime.now)

  val registry = TestActorRef(new Actor {
    override def receive = {
      case FindByName("tester") =>
        sender ! LookupResult(Seq(ingestorInfo))
      case FindAll =>
        sender ! LookupResult(Seq(ingestorInfo))
    }
  }, "ingestor_registry")


  val props = IngestionHandlerGateway.props(registry.path.toString)

  val gateway = system.actorOf(props)

  implicit override val patienceConfig = PatienceConfig(timeout = Span(5, Seconds),
    interval = Span(1, Seconds))

  "The IngestionHandlerGateway when part of a cluster" should "receive published messages" in {
    val topic = IngestionHandlerGateway.TopicName
    val request = ingest.HydraRequest("123", "payload")
    val msg = akka.cluster.pubsub.DistributedPubSubMediator.Publish(topic,
      InitiateRequest(request, 1.second), true)
    mediator ! msg

    expectMsgPF() {
      case IngestionReport(c, _, statusCode) =>
        c shouldBe "123"
        statusCode shouldBe 200
    }
  }
}
