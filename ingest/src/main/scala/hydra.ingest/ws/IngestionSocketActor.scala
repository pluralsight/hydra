package hydra.ingest.ws

/**
  * Created by alexsilva on 3/10/17.
  */

import akka.actor.Actor
import hydra.common.logging.LoggingAdapter
import hydra.core.ingest.IngestionParams._
import hydra.ingest.ws.chat._

class IngestionSocketActor(destination: String) extends Actor with LoggingAdapter {

  private var ingestionMetadata: Map[String, Any] = Map.empty[String, Any]

  val paramList = Seq(HYDRA_INGESTOR_PARAM, HYDRA_INGESTOR_TARGET_PARAM, HYDRA_REQUEST_LABEL_PARAM
    , RETRY_STRATEGY_PARAM, HYDRA_RECORD_FORMAT_PARAM, HYDRA_VALIDATION_PARAM)

  override def receive: Receive = {
    case UserJoined(name, actorRef) =>
      broadcast(SystemMessage(s"User $name joined channel..."))
      println(s"User $name joined channel[$destination]")

    case UserLeft(name) =>
      println(s"User $name left channel[$destination]")
      broadcast(SystemMessage(s"User $name left channel[$destination]"))

    case msg: IncomingMessage =>
      if(msg.message.startsWith("--set")) {
        val value = msg.message.substring(5).split(" ")
        log.debug(s"Setting property ${value(0).toUpperCase} to ${value(1)}")
        ingestionMetadata += value(0).toUpperCase -> value(1)
      }
      broadcast(msg)
  }

  def broadcast(message: ChatMessage): Unit = {}

}
