package hydra.ingest.services

import akka.NotUsed
import akka.actor.{ActorRef, ActorRefFactory, Props}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Sink, Source}
import hydra.core.ingest.IngestionReport

trait IngestSocketFactory {
  def ingestFlow(): Flow[String, OutgoingMessage, NotUsed]
}

object IngestSocketFactory {

  def createSocket(fact: ActorRefFactory): IngestSocketFactory = { () =>
    {

      val socketActor = fact.actorOf(Props[IngestionSocketActor])

      def actorSink =
        Sink.actorRefWithBackpressure(
          socketActor,
          onInitMessage = SocketInit,
          ackMessage = SocketAck,
          onCompleteMessage = SocketEnded,
          onFailureMessage = SocketFailed.apply
        )

      val in =
        Flow[String]
          .map(IncomingMessage)
          .to(actorSink)

      val out =
        Source
          .actorRefWithBackpressure[OutgoingMessage](
            SocketAck,
            PartialFunction.empty,
            PartialFunction.empty
          )
          .mapMaterializedValue(socketActor ! SocketStarted(_))

      Flow.fromSinkAndSourceCoupled(in, out)

    }
  }
}

sealed trait SocketEvent

case object SocketInit extends SocketEvent

case class SocketStarted(ref: ActorRef) extends SocketEvent

case object SocketEnded extends SocketEvent

case object SocketAck extends SocketEvent

case class IncomingMessage(message: String) extends SocketEvent

case class SocketFailed(ex: Throwable)

sealed trait OutgoingMessage extends SocketEvent

case class SimpleOutgoingMessage(status: Int, message: String)
    extends OutgoingMessage

case class IngestionOutgoingMessage(report: IngestionReport)
    extends OutgoingMessage
