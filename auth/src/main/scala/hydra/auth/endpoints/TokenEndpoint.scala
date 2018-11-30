package hydra.auth.endpoints

import akka.actor._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.auth.actors.TokenActor
import hydra.common.logging.LoggingAdapter
import hydra.core.http.HydraDirectives
import hydra.core.marshallers.HydraJsonSupport

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


class TokenEndpoint(implicit val system: ActorSystem, implicit val e: ExecutionContext)
  extends RoutedEndpoints with LoggingAdapter with HydraJsonSupport with HydraDirectives {

  private implicit val timeout = Timeout(10.seconds)

  private implicit val mat = ActorMaterializer()

  private val tokenActor = system.actorOf(
    TokenActor.props())

  override val route: Route =
    pathPrefix("token") {
      pathEndOrSingleSlash {
        get {
          complete(StatusCodes.OK)
        }
      }
    }
}
