package hydra.ingest.modules

import akka.actor.ActorSystem
import cats.effect.Sync
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.util.Futurable
import hydra.ingest.app.AppConfig.CreateTopicConfig
import hydra.kafka.endpoints.BootstrapEndpointV2

import scala.concurrent.ExecutionContext

final class HttpApi[F[_]] private(
  programs: Programs[F],
  cfg: CreateTopicConfig
)(implicit futurable: Futurable[F, Unit], system: ActorSystem, ec: ExecutionContext) {
  def getRoutes: List[RoutedEndpoints] = {
    getCreateMetadataV2Route.toList
  }

  def getCreateMetadataV2Route: Option[RoutedEndpoints] = {
    if (cfg.v2CreateEndpointEnabled) {
      Some(new BootstrapEndpointV2(programs.createTopic))
    } else {
      None
    }
  }
}

object HttpApi {

  def make[F[_]: Sync](programs: Programs[F],
                       createTopicConfig: CreateTopicConfig)
                      (implicit futurable: Futurable[F, Unit], system: ActorSystem, ec: ExecutionContext): F[HttpApi[F]] = Sync[F].delay {
    new HttpApi[F](programs, createTopicConfig)
  }

}
