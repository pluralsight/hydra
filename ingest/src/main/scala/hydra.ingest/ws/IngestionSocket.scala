package hydra.ingest.ws

import akka.actor.{ActorRefFactory, Props}
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl._
import akka.stream.{FlowShape, OverflowStrategy}

/**
  * Created by alexsilva on 3/7/17.
  */
class IngestionSocket(fact: ActorRefFactory) {

  val socketActor = fact.actorOf(Props(classOf[IngestionSocketActor]))

  def ingestionWSFlow1(user: String): Flow[Message, Message, _] = {
    val source = Source.actorRef[SocketMessage](bufferSize = 5, OverflowStrategy.fail)
    Flow.fromGraph(GraphDSL.create(source) {
      implicit builder =>
        wsSource =>
          val fromWebsocket = builder.add(
            Flow[Message].collect {
              case TextMessage.Strict(txt) => IncomingMessage(txt)
            })

          val backToWebsocket = builder.add(
            Flow[SocketMessage].map {
              case SocketMessage(author, text) => TextMessage(s"[$author]: $text")
            }
          )


          //merges both pipes
          val merge = builder.add(Merge[ChatEvent](2))

          //Materialized value of Actor who sit in chatroom
          val actorAsSource = builder.materializedValue.map(actor => SocketStarted(user))

          //send messages to the actor, if send also UserLeft(user) before stream completes.
          val chatActorSink = Sink.actorRef[ChatEvent](socketActor, SocketEnded(user))

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


//  def ingestionWSFlow(user: String): Flow[Message, Message, _] = {
//    val source = Source.actorRef[SocketMessage](bufferSize = 5, OverflowStrategy.fail)
//    Flow.fromGraph(GraphDSL.create(source) {
//      implicit builder =>
//        wsSource => //source provided as argument
//
//          //flow used as input it takes Message's
//          val fromWebsocket = builder.add(
//            Flow[Message].collect {
//              case TextMessage.Strict(txt) => IncomingMessage(txt)
//            })
//
//          //flow used as output, it returns Message's
//          val backToWebsocket = builder.add(
//            Flow[SocketMessage].map {
//              case SocketMessage(author, text) => TextMessage(s"[$author]: $text")
//            }
//          )
//
//
//          //send messages to the actor, if send also UserLeft(user) before stream completes.
//          val chatActorSink = Sink.actorRef[ChatEvent](socketActor, SocketEnded(user))
//
//          //merges both pipes
//          val merge = builder.add(Merge[ChatEvent](2))
//
//          //Materialized value of Actor who sit in chatroom
//          val actorAsSource = builder.materializedValue.map(actor => UserJoined(user, actor))
//
//          //Message from websocket is converted into IncommingMessage and should be send to each in room
//          fromWebsocket ~> merge.in(0)
//
//          //If Source actor is just created should be send as UserJoined and registered as particiant in room
//          actorAsSource ~> merge.in(1)
//
//          //Merges both pipes above and forward messages to chatroom Represented by ChatRoomActor
//          merge ~> chatActorSink
//
//          //Actor already sit in chatRoom so each message from room is used as source and pushed back into websocket
//          wsSource ~> backToWebsocket
//
//          // expose ports
//          FlowShape.of(fromWebsocket.in, backToWebsocket.out)
//    })
//  }


}

object IngestionSocket {
  def apply()(implicit fact: ActorRefFactory) = new IngestionSocket(fact)
}