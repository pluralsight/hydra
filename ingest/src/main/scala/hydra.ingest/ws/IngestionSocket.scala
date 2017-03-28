package hydra.ingest.ws

import akka.actor.{ActorRefFactory, Props}
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl._
import akka.stream.{FlowShape, OverflowStrategy}
import spray.json.DefaultJsonProtocol

/**
  * Created by alexsilva on 3/7/17.
  */
class IngestionSocket(fact: ActorRefFactory) extends DefaultJsonProtocol {

  import spray.json._

  implicit val outgoingMessageFormat = jsonFormat2(OutgoingMessage)

  val socketActor = fact.actorOf(Props(classOf[IngestionSocketActor]))

  def ingestionWSFlow(user: String): Flow[Message, Message, _] = {
    val source = Source.actorRef[OutgoingMessage](bufferSize = 5, OverflowStrategy.fail)
    Flow.fromGraph(GraphDSL.create(source) {
      implicit builder =>
        wsSource =>
          val fromWebsocket = builder.add(
            Flow[Message].collect {
              case TextMessage.Strict(txt) => IncomingMessage(txt)
            })

          val backToWebsocket = builder.add(
            Flow[OutgoingMessage].map {
              case m: OutgoingMessage => TextMessage(m.toJson.compactPrint)
            }
          )

          //merges both pipes
          val merge = builder.add(Merge[SocketEvent](2))

          val actorAsSource = builder.materializedValue.map(actor => SocketStarted(user, actor))

          //send messages to the actor, if send also UserLeft(user) before stream completes.
          val chatActorSink = Sink.actorRef[SocketEvent](socketActor, SocketEnded)

          //Message from websocket is converted into IncommingMessage and should be send to each in room
          fromWebsocket ~> merge.in(0)

          //If Source actor is just created should be send as UserJoined and registered as particiant in room
          actorAsSource ~> merge.in(1)

          //Merges both pipes above and forward messages to chatroom Represented by ChatRoomActor
          merge ~> chatActorSink

          //Actor already sit in chatRoom so each message from room is used as source and pushed back into websocket
          wsSource ~> backToWebsocket

          // expose ports
          FlowShape.of(fromWebsocket.in, backToWebsocket.out)
    })
  }
}

object IngestionSocket {
  def apply()(implicit fact: ActorRefFactory) = new IngestionSocket(fact)
}