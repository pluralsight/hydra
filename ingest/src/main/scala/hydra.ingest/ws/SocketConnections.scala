package hydra.ingest.ws

import akka.actor.ActorRefFactory

/**
  * Created by alexsilva on 3/7/17.
  */
object SocketConnections {
  private var connections: Map[String, IngestionSocket] = Map.empty[String, IngestionSocket]

  def findOrCreate(label: String)(implicit factory: ActorRefFactory): IngestionSocket =
    connections.getOrElse(label, createNewConnection(label))

  private def createNewConnection(label: String)(implicit factory: ActorRefFactory): IngestionSocket = {
    val connection = IngestionSocket()
    connections += label -> connection
    connection
  }
}