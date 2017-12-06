package hydra.rabbit

import hydra.core.ingest.{Ingestor, TransportOps}
import RabbitRecord._
import hydra.core.protocol._

class RabbitIngestor extends Ingestor with TransportOps {
  override def recordFactory = RabbitRecordFactory

  override def transportName = "rabbit"

  ingest {
    case Publish(request) =>
      val exists = request.hasMetadata(HYDRA_RABBIT_EXCHANGE) || request.hasMetadata(HYDRA_RABBIT_QUEUE)
      sender ! (if(exists) Join else Ignore)

    case Ingest(record, ack) =>
      transport(record, ack)

  }

}