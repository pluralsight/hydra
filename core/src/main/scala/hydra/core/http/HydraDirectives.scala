package hydra.core.http

import akka.NotUsed
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.directives.SecurityDirectives
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import configs.syntax._
import hydra.common.config.ConfigSupport

import scala.concurrent.Promise

/**
  * Created by alexsilva on 2/7/17.
  */
trait HydraDirectives extends Directives with ConfigSupport {

  type HydraAuthenticator = hydra.core.http.Authenticator[String]

  val authClass = applicationConfig.get[String]("http.authenticator").valueOrElse(classOf[SimpleAuthenticator].getName)

  val authenticator: HydraAuthenticator = Class.forName(authClass).newInstance().asInstanceOf[HydraAuthenticator]

  val authenticate: Directive1[String] =
    SecurityDirectives.authenticateBasic(realm = "Hydra", authenticator.authenticate)


  def imperativelyComplete(inner: ImperativeRequestContext => Unit): Route = { ctx: RequestContext =>
    val p = Promise[RouteResult]()
    inner(new ImperativeRequestContext(ctx, p))
    p.future
  }

  def completeWithLocationHeader[T](status: StatusCode, resourceId: T): Route =
    extractRequestContext { requestContext =>
      val request = requestContext.request
      val location = request.uri.copy(path = request.uri.path / resourceId.toString)
      respondWithHeader(Location(location)) {
        complete(status)
      }
    }
}

class SideEffector[A, B](onComplete: B => Unit, onFailure: Throwable => Unit, zero: B, acc: (B, A) => B)
  extends GraphStage[FlowShape[A, A]] {

  val in = Inlet[A]("SideEffector.in")
  val out = Outlet[A]("SideEffector.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var accData = zero
      var called = false

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val data = grab(in)
          accData = acc(accData, data)
          push(out, data)
        }

        override def onUpstreamFinish(): Unit = {
          called = true
          onComplete(accData)
          super.onUpstreamFinish()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          onFailure(ex)
          super.onUpstreamFailure(ex)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)

        override def onDownstreamFinish(): Unit = {
          called = true
          onComplete(accData)
          super.onDownstreamFinish()
        }
      })
    }
}

object SideEffector {
  def flow[A, B](onComplete: B => Unit, onFailure: Throwable => Unit = _ => ())(zero: B)(acc: (B, A) => B): Flow[A, A, NotUsed] =
    Flow.fromGraph(new SideEffector(onComplete, onFailure, zero, acc))
}


// an imperative wrapper for request context
final class ImperativeRequestContext(val ctx: RequestContext, promise: Promise[RouteResult]) {
  private implicit val ec = ctx.executionContext

  def complete(obj: ToResponseMarshallable): Unit = ctx.complete(obj).onComplete(promise.complete)

  def failWith(error: Throwable): Unit = ctx.fail(error).onComplete(promise.complete)
}