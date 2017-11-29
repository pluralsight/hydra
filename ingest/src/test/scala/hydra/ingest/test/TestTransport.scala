package hydra.ingest.test

import hydra.core.transport.Transport
import hydra.core.transport.TransportSupervisor.Deliver

/**
  * Created by alexsilva on 3/26/17.
  */
class TestTransport extends Transport {

  override def receive: Receive = {
    case Deliver(record, deliveryId, callback) => sender ! "DONE"
  }
}

