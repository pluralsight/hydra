package hydra.ingest.services

import akka.actor.{Actor, ActorSystem, Props}
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.testkit.{ImplicitSender, TestKit}
import hydra.core.http.{HydraDirectives, ImperativeRequestContext}
import hydra.core.ingest.{HydraRequest, IngestionReport}
import hydra.core.protocol._
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/9/17.
  */
class IngestionRequestHandlerSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike
  with ImplicitSender with BeforeAndAfterAll with HydraDirectives with Eventually {

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true, duration = 10 seconds)

  val req = HydraRequest("123", "test payload")

  describe("When starting an HTTP ingestion") {
    it("completes the request with 400") {
      val ctx = new ImperativeRequestContext {
        var completed: ToResponseMarshallable = _
        var error: Throwable = _

        override def complete(obj: ToResponseMarshallable): Unit = completed = obj

        override def failWith(error: Throwable): Unit = this.error = error
      }
      system.actorOf(IngestionRequestHandler.props(req, Props(classOf[DummySupervisor],
        req.withCorrelationId("2")), 3.seconds, ctx))
      eventually {
        ctx.completed should not be null
      }
      ctx.completed.value.asInstanceOf[(_, _)]._2 match {
        case IngestionReport(c, i, statusCode, _) =>
          c shouldBe "123"
          statusCode shouldBe 400
      }
    }

    it("completes the request with a 200") {
      val ctx = new ImperativeRequestContext {
        var completed: ToResponseMarshallable = _
        var error: Throwable = _

        override def complete(obj: ToResponseMarshallable): Unit = completed = obj

        override def failWith(error: Throwable): Unit = this.error = error
      }
      system.actorOf(IngestionRequestHandler.props(req.withCorrelationId("a44"),
        Props(classOf[DummySupervisor], req.withCorrelationId("a44")), 3.seconds, ctx))
      eventually {
        ctx.completed should not be null
      }
      ctx.completed.value.asInstanceOf[(_, _)]._2 match {
        case IngestionReport(c, _, statusCode, _) =>
          c shouldBe "a44"
          statusCode shouldBe 200
      }
    }

    it("fails the request on ingestion error") {
      val ctx = new ImperativeRequestContext {
        var completed: ToResponseMarshallable = _
        var error: Throwable = _

        override def complete(obj: ToResponseMarshallable): Unit = completed = obj

        override def failWith(error: Throwable): Unit = this.error = error
      }
      system.actorOf(IngestionRequestHandler
        .props(req.withCorrelationId("1"), Props(classOf[DummySupervisor], req.withCorrelationId("1")),
          3.seconds, ctx))
      Thread.sleep(1000)
      eventually {
        ctx.error should not be null
      }
      ctx.error shouldBe an[IllegalArgumentException]
    }
  }
}

private class DummySupervisor(r: HydraRequest) extends Actor {

  val req = HydraRequest("123", "test payload")

  if (r.correlationId == "1") {
    context.parent ! HydraIngestionError("dummy_ingestor", new IllegalArgumentException, r)
  }
  else if (r.correlationId == "2") {
    //matches the _ in RequestHandler
    context.parent ! "Something else"
  }
  else {
    val i = IngestionReport(r.correlationId, Map.empty, 200)
    context.parent ! i
  }

  override def receive = {
    case Publish(_) =>
    case Validate(_) => context.parent ! ValidRequest
    case Ingest(r, _) => context.parent ! HydraIngestionError("dummy_ingestor", new IllegalArgumentException, req)
  }
}