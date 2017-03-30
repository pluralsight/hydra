package hydra.ingest.ws

import akka.actor.{ActorRefFactory, Props}
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl._
import akka.stream.{FlowShape, OverflowStrategy}
import akka.util.ByteString
import hydra.ingest.marshallers.IngestionJsonSupport

/**
  * Created by alexsilva on 3/7/17.
  */
class IngestionSocket(fact: ActorRefFactory, metadata: Map[String, String]) extends IngestionJsonSupport {

  import spray.json._

  implicit val simpleOutgoingMessageFormat = jsonFormat2(SimpleOutgoingMessage)

  val socketActor = fact.actorOf(Props(classOf[IngestionSocketActor], metadata))

  def ingestionWSFlow(ingestorName: String): Flow[Message, Message, _] = {
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
              case m: SimpleOutgoingMessage => TextMessage(m.toJson.compactPrint)
              case r: IngestionOutgoingMessage => TextMessage(r.report.toJson.compactPrint)
            }
          )

          //merges both pipes
          val merge = builder.add(Merge[SocketEvent](2))

          val actorAsSource = builder.materializedValue.map(actor => SocketStarted(ingestorName, actor))

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
  def apply(metadata: Map[String, String])(implicit fact: ActorRefFactory) = new IngestionSocket(fact, metadata)
}