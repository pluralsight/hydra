package hydra.examples.produce

import java.nio.file.Paths

import akka.stream.scaladsl.{FileIO, Flow, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, IOResult}
import akka.util.ByteString
import hydra.core.transport.Transport
import hydra.core.protocol.Produce
import org.reactivestreams.{Publisher, Subscriber}

import scala.concurrent.Future

/**
  * Created by alexsilva on 3/29/17.
  */
class FileTransport extends Transport with Publisher[String] {

  implicit val materializer = ActorMaterializer()

  private var subscriber: Option[Subscriber[_ >: String]] = None

  /**
    * This should be loaded from a config.
    */
  private val destinations = Map("default" ->
    Source.fromPublisher(this).runWith(messageSink("/tmp/hydra-producer.txt")))


  transport {
    case Produce(r: FileRecord) =>
      destinations.get(r.destination).map { flow =>
        subscriber.map(_.onNext(r.payload))
      }
  }

  def messageSink(fileName: String): Sink[String, Future[IOResult]] =
    Flow[String]
      .map(s => ByteString(s + "\n"))
      .toMat(FileIO.toPath(Paths.get(fileName)))(Keep.right)

  override def subscribe(s: Subscriber[_ >: String]): Unit = {
    subscriber = Some(s)
  }
}
