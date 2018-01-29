package hydra.core.connect

import akka.actor.ActorSystem
import com.typesafe.config.Config
import configs.syntax._
import hydra.core.ingest.RequestParams.{HYDRA_ACK_STRATEGY, HYDRA_VALIDATION_STRATEGY}
import hydra.core.transport.{AckStrategy, ValidationStrategy}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 12/17/15.
  */
class ConnectorSettings(config: Config, system: ActorSystem) {

  final val requestTimeout = config.get[FiniteDuration]("request.timeout")
    .valueOrElse(2.seconds)

  final val metadata: Map[String, String] = config.get[Config]("request.metadata")
    .map(hydra.common.config.ConfigSupport.toMap).valueOrElse(Map.empty).map(e => e._1 -> e._2.toString)

  final val validationStrategy = config.get[String](HYDRA_VALIDATION_STRATEGY)
    .map(ValidationStrategy.apply).valueOrElse(ValidationStrategy.Strict)

  final val ackStrategy = config.get[String](HYDRA_ACK_STRATEGY)
    .map(AckStrategy.apply).valueOrElse(AckStrategy.NoAck)

  final val charset = config.get[String]("charset").valueOrElse("UTF-8")

  final val clustered = system.settings.ProviderClass == "akka.cluster.ClusterActorRefProvider"
}