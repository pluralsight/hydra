package hydra.ingest.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Route
import hydra.core.http.RouteSupport
import hydra.ingest.bootstrap.BuildInfo
import spray.json.DefaultJsonProtocol

object HealthEndpoint extends RouteSupport with DefaultJsonProtocol with SprayJsonSupport {
  override val route: Route =
    path("health") {
      pathEndOrSingleSlash {
        get(complete(BuildInfo.toJson))
      }
    }
}