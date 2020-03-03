package hydra.ingest.test

import hydra.core.ingest.HydraRequest
import hydra.core.transport.{AckStrategy, HydraRecord, RecordFactory}

import scala.concurrent.{ExecutionContext, Future}

object TestRecordFactory extends RecordFactory[String, String] {

  override def build(r: HydraRequest)(implicit ec: ExecutionContext) = {
    val timeout = r.metadataValueEquals("timeout", "true")
    if (timeout) {
      Future.successful(
        TimeoutRecord(
          "test-topic",
          r.correlationId.toString,
          r.payload,
          r.ackStrategy
        )
      )
    } else {
      Future.successful(
        TestRecord(
          "test-topic",
          r.correlationId.toString,
          r.payload,
          r.ackStrategy
        )
      )
    }
  }
}

case class TestRecord(
    destination: String,
    key: String,
    payload: String,
    ackStrategy: AckStrategy
) extends HydraRecord[String, String]

case class TimeoutRecord(
    destination: String,
    key: String,
    payload: String,
    ackStrategy: AckStrategy
) extends HydraRecord[String, String]
