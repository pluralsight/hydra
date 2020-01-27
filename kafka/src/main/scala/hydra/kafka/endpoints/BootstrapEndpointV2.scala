package hydra.kafka.endpoints

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints

import scala.concurrent.ExecutionContext

class BootstrapEndpointV2(implicit val system: ActorSystem, implicit val e: ExecutionContext) extends RoutedEndpoints {
  override val route: Route = {
    pathPrefix("streams") {
      put {
        pathEndOrSingleSlash {
          complete(StatusCodes.OK)
        }
      }
    }
  }
}
