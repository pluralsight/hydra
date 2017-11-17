package hydra.sandbox.transport

import java.nio.file.Paths
import java.nio.file.StandardOpenOption.{APPEND, CREATE}

import akka.actor.{Actor, Props}
import akka.stream._
import akka.stream.scaladsl.{FileIO, Flow, Sink, Source}
import akka.util.ByteString
import hydra.core.protocol._

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
    case Produce(r: FileRecord, supervisor, ack) =>
      sinks.get(r.destination).map { flow =>
        val ingestor = sender
        val f = flow.offer(r.payload)
        f.onComplete {
          case Success(_) =>
            //todo: look at the QueueOfferResult object
            val md = FileRecordMetadata(destinations(r.destination), 0)
            ingestor ! RecordProduced(md, supervisor)
          case Failure(ex) => ingestor ! RecordNotProduced(r, ex, supervisor)
        }
      }.getOrElse(sender ! RecordNotProduced(r,
        new IllegalArgumentException(s"File stream ${r.destination} not found."), supervisor))
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