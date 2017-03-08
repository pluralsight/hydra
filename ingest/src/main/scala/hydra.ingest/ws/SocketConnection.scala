package hydra.ingest.ws

import akka.actor.{ActorRefFactory, Props}
import akka.http.scaladsl.model.ws.Message
import akka.stream.scaladsl.Flow

/**
  * Created by alexsilva on 3/7/17.
  */
class SocketConnection(connectionId: Int, actorRefFactory: ActorRefFactory) {

//  private[this] val webSocketActor = actorRefFactory.actorOf(Props(classOf[ChatRoomActor], connectionId))
//
//  def websocketFlow(user: String): Flow[Message, Message, _] = ???
//
//  def sendMessage(message: ChatMessage): Unit = chatRoomActor ! message

}

object SocketConnection {
  def apply(connectionId: Int)(implicit actorRefFactory: ActorRefFactory) =
    new SocketConnection(connectionId, actorRefFactory)
}
