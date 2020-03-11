package hydra.ingest.services

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorSystem}
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.pattern.pipe
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import hydra.core.http.ImperativeRequestContext
import hydra.core.ingest.{HydraRequest, IngestionReport}
import hydra.core.protocol._
import hydra.ingest.IngestorInfo
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName, LookupResult}
import hydra.ingest.test.TestRecordFactory
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class IngestionHandlerGatewaySpec
    extends TestKit(ActorSystem("hydra"))
    with Matchers
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with Eventually
    with ImplicitSender {

  override def afterAll =
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10 seconds
    )

  val ingestor = TestActorRef(
    new Actor {

      override def receive = {
        case Publish(_) => sender ! Join
        case Validate(r) =>
          TestRecordFactory.build(r).map(ValidRequest(_)) pipeTo sender
        case Ingest(r, _) => sender ! IngestorCompleted
      }
    },
    "test_ingestor"
  )

  val ingestorInfo =
    IngestorInfo("test_ingestor", "test", ingestor.path, DateTime.now)

  val registry = TestActorRef(
    new Actor {

      override def receive = {
        case FindByName("tester") =>
          sender ! LookupResult(Seq(ingestorInfo))
        case FindAll =>
          sender ! LookupResult(Seq(ingestorInfo))
      }
    },
    "ingestor_registry"
  )

  val props = IngestionHandlerGateway.props(registry.path.toString)

  val gateway = system.actorOf(props)

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(1, Seconds))

  "The IngestRequestGateway actor" should "complete an http request" in {

    val ctx = new ImperativeRequestContext {
      var completed: ToResponseMarshallable = _
      var error: Throwable = _

      override def complete(obj: ToResponseMarshallable): Unit = completed = obj

      override def failWith(error: Throwable): Unit = this.error = error
    }

    val request = HydraRequest("123", "test payload")
    gateway ! InitiateHttpRequest(request, 1 second, ctx)
    eventually {
      ctx.completed should not be null
    }
    ctx.completed.value.asInstanceOf[(_, _)]._2 match {
      case IngestionReport(c, _, statusCode) =>
        c shouldBe "123"
        statusCode shouldBe 200
    }
  }

  it should "stop on any exception" in {
    val supervisor = TestActorRef[IngestionHandlerGateway](props)
    val strategy = supervisor.underlyingActor.supervisorStrategy.decider
    strategy(new IllegalArgumentException("boom")) should be(Stop)
  }

  it should "complete an actor request" in {
    val request = HydraRequest("123", "test payload")
    gateway ! InitiateRequest(request, 1.second)
    expectMsgPF() {
      case IngestionReport(c, _, statusCode) =>
        c shouldBe "123"
        statusCode shouldBe 200
    }
  }

}
