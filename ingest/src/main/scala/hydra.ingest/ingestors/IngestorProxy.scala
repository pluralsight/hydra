package hydra.ingest.ingestors

import akka.actor.Actor
import com.typesafe.config.Config

class IngestorProxy(config: Config) extends Actor {

  override def receive: Receive = {
    case _ =>
  }
}


object IngestorProxy {

  case class CreateProxy(proxy: IngestorProxyConfig)

}


case class IngestorProxyConfig(name: String,
                               schema: String, metadata: Map[String, String])
