package hydra.ingest.test

import hydra.core.ingest.Ingestor

import scala.concurrent.duration._

class TestIngestorDefault extends Ingestor {

  /**
    * This will _not_ override; instead it will use the default value of 1.second. We'll test it.
    */
  override val initTimeout = 2.millisecond

  val to = context.receiveTimeout

  ingest {
    case "hello" => sender ! "hi!"
    case "timeout" => sender ! to
  }

  override val recordFactory = TestRecordFactory
}