package hydra.core.http

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.directives.SecurityDirectives
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

// an imperative wrapper for request context
final class ImperativeRequestContext(val ctx: RequestContext, promise: Promise[RouteResult]) {
  private implicit val ec = ctx.executionContext

  def complete(obj: ToResponseMarshallable): Unit = ctx.complete(obj).onComplete(promise.complete)

  def failWith(error: Throwable): Unit = ctx.fail(error).onComplete(promise.complete)
}