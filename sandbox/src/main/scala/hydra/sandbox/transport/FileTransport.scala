package hydra.sandbox.transport

import java.nio.file.Paths
import java.nio.file.StandardOpenOption.{APPEND, CREATE}

import akka.stream._
import akka.stream.scaladsl.{FileIO, Flow, Sink, Source}
import akka.util.ByteString
import hydra.core.protocol.Produce
import hydra.core.transport.Transport

import scala.concurrent.Future

/**
  * Created by alexsilva on 3/29/17.
  */
class FileTransport extends Transport {

  implicit val materializer = ActorMaterializer()

  /**
    * This should be loaded from a config.
    */
  private val destinations = Map("default" ->
    Source.queue(0, OverflowStrategy.backpressure).to(messageSink("/tmp/hydra-sandbox.txt")).run())

  transport {
    case Produce(r: FileRecord, _, _, _) =>
      destinations.get(r.destination).map { flow =>
        flow.offer(r.payload)
      }
  }

  def messageSink(fileName: String): Sink[String, Future[IOResult]] = {
    val fileSink = FileIO.toPath(Paths.get(fileName), Set(APPEND, CREATE))
    Flow[String]
      .map(s => ByteString(s + "\n"))
      .toMat(fileSink)((_, bytesWritten) => bytesWritten)
  }
}