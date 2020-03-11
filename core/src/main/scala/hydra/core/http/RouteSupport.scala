package hydra.core.http

/**
  * Created by alexsilva on 3/28/17.
  */
import akka.http.scaladsl.server.Route
import hydra.common.logging.LoggingAdapter
import hydra.core.marshallers.HydraJsonSupport

trait RouteSupport
    extends HydraJsonSupport
    with LoggingAdapter
    with HydraDirectives {
  def route: Route
}
