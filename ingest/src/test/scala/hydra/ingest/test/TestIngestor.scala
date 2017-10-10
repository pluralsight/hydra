package hydra.ingest.test

import hydra.core.ingest.Ingestor
import hydra.core.protocol.{Ingest, IngestorCompleted, Join, Publish}
import hydra.core.transport.RecordFactory

/**
  * Created by alexsilva on 3/26/17.
  */
class TestIngestor extends Ingestor {

  override def recordFactory: RecordFactory[_, _] = TestRecordFactory

  ingest {
    case Publish(_) =>
      sender ! Join

    case Ingest(request) =>
      log.info(request.payload.toString)
      sender ! IngestorCompleted
  }
}
