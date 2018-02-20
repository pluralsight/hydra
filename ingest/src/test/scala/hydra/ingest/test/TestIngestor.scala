package hydra.ingest.test

import hydra.core.ingest.Ingestor
import hydra.core.protocol.{Ingest, IngestorCompleted, Join, Publish}
import hydra.core.transport.RecordFactory
import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/26/17.
  */
class TestIngestor extends Ingestor {

  override def initTimeout = 2.seconds

  override def recordFactory: RecordFactory[_, _] = TestRecordFactory

  ingest {
    case Publish(_) =>
      sender ! Join

    case Ingest(request, _) =>
      log.info(Option(request.payload).getOrElse("<<EMPTY PAYLOAD>>").toString)
      sender ! IngestorCompleted
  }
}
