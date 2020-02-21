package hydra.core.http

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server._
import hydra.common.config.ConfigSupport

import scala.concurrent.Promise

/**
  * Created by alexsilva on 2/7/17.
  */
trait HydraDirectives extends Directives with ConfigSupport {

  def imperativelyComplete(inner: ImperativeRequestContext => Unit): Route = {
    ctx: RequestContext =>
      val p = Promise[RouteResult]()
      inner(new ImperativeRequestContextImpl(ctx, p))
      p.future
  }

  def completeWithLocationHeader[T](status: StatusCode, resourceId: T): Route =
    extractRequestContext { requestContext =>
      val request = requestContext.request
      val location = request.uri.withPath(Path("/" + resourceId.toString))
      respondWithHeader(Location(location)) {
        complete(status)
      }
    }
}

trait ImperativeRequestContext {
  def complete(obj: ToResponseMarshallable): Unit

  def failWith(error: Throwable): Unit
}

// an imperative wrapper for request context
final class ImperativeRequestContextImpl(
    val ctx: RequestContext,
    promise: Promise[RouteResult]
) extends ImperativeRequestContext {

  private implicit val ec = ctx.executionContext

  def complete(obj: ToResponseMarshallable): Unit =
    ctx.complete(obj).onComplete(promise.complete)

  def failWith(error: Throwable): Unit =
    ctx.fail(error).onComplete(promise.complete)
}
