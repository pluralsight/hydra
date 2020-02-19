package hydra.common.util

import akka.http.scaladsl.server.Route

abstract class RoutedEndpointLookup {
  def route: Route
}
