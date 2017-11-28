package hydra.sandbox.transport

import java.nio.file.Paths
import java.nio.file.StandardOpenOption.{APPEND, CREATE}

import akka.actor.{Actor, Props}
import akka.stream._
import akka.stream.scaladsl.{FileIO, Flow, Sink, Source}
import akka.util.ByteString
import hydra.core.transport.Transport.Deliver

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Created by alexsilva on 3/29/17.
  */
class FileTransport(destinations: Map[String, String]) extends Actor {

  implicit val materializer = ActorMaterializer()

  private implicit val ec = context.dispatcher

  private val sharedKillSwitch = KillSwitches.shared("file-transport-switch")

  private val sinks = destinations.map { e =>
    e._1 -> Source.queue(0, OverflowStrategy.backpressure).to(toMessageSink(e._2)).run()
  }

  override def postStop(): Unit = {
    sharedKillSwitch.shutdown()
  }

  override def receive: Receive = {
    case Deliver(r: FileRecord, deliveryId, callback) =>
      sinks.get(r.destination).map { flow =>
        val f = flow.offer(r.payload)
        f.onComplete {
          case Success(_) =>
            //todo: look at the QueueOfferResult object
            val md = FileRecordMetadata(destinations(r.destination), 0)
            callback.onCompletion(deliveryId, Some(md), None)
          case Failure(ex) => callback.onCompletion(deliveryId, None, Some(ex))
        }
      }.getOrElse(
        callback.onCompletion(deliveryId, None,
        Some(new IllegalArgumentException(s"File stream ${r.destination} not found."))))
  }


  private def toMessageSink(fileName: String): Sink[String, Future[IOResult]] = {
    val fileSink = FileIO.toPath(Paths.get(fileName), Set(APPEND, CREATE))
    Flow[String]
      .map(s => ByteString(s + "\n"))
      .via(sharedKillSwitch.flow)
      .toMat(fileSink)((_, bytesWritten) => bytesWritten)
  }
}

object FileTransport {
  def props(destinations: Map[String, String]): Props = Props(classOf[FileTransport], destinations)
}