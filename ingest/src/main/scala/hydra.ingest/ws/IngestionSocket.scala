package hydra.ingest.ws

import akka.actor.{ActorRefFactory, Props}
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl._
import akka.stream.{FlowShape, OverflowStrategy}
import hydra.ingest.marshallers.IngestionJsonSupport

/**
  * Created by alexsilva on 3/7/17.
  */
class IngestionSocket(fact: ActorRefFactory, metadata: Map[String, String]) extends IngestionJsonSupport {

  import spray.json._

  implicit val simpleOutgoingMessageFormat = jsonFormat2(SimpleOutgoingMessage)

  def ingestionWSFlow(ingestorName: String): Flow[Message, Message, _] = {
    val socketActor = fact.actorOf(Props(classOf[IngestionSocketActor], metadata))

    val source = Source.actorRef[OutgoingMessage](bufferSize = 5, OverflowStrategy.fail)
    Flow.fromGraph(GraphDSL.create(source) {
      implicit builder =>
        wsSource =>
          val fromWS = builder.add(
            Flow[Message].collect {
              case TextMessage.Strict(txt) => IncomingMessage(txt)
            })

          val backToWS = builder.add(
            Flow[OutgoingMessage].map {
              case m: SimpleOutgoingMessage => TextMessage(m.toJson.compactPrint)
              case r: IngestionOutgoingMessage => TextMessage(r.report.toJson.compactPrint)
            }
          )

          val merge = builder.add(Merge[SocketEvent](2))

          val actorAsSource = builder.materializedValue.map(actor => SocketStarted(ingestorName, actor))

          val socketSink = Sink.actorRef[SocketEvent](socketActor, SocketEnded)

          fromWS ~> merge.in(0)

          actorAsSource ~> merge.in(1)

          merge ~> socketSink

          wsSource ~> backToWS

          FlowShape.of(fromWS.in, backToWS.out)
    })
  }
}

object IngestionSocket {
  def apply(metadata: Map[String, String])(implicit fact: ActorRefFactory) = new IngestionSocket(fact, metadata)
}