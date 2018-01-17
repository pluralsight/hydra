package hydra.kafka.test

import hydra.core.ingest.HydraRequest
import hydra.core.transport.{HydraRecord, RecordFactory}

import scala.concurrent.{ExecutionContext, Future}

object TestRecordFactory extends RecordFactory[String, String] {
  override def build(r: HydraRequest)(implicit ec:ExecutionContext) = {
    val timeout = r.metadataValueEquals("timeout", "true")
    if (timeout) {
      Future.successful(TimeoutRecord("test-topic", Some(r.correlationId.toString), r.payload))
    }
    else {
      Future.successful(TestRecord("test-topic", Some(r.correlationId.toString), r.payload))
    }
  }
}

case class TestRecord(destination: String,
                      key: Option[String],
                      payload: String) extends HydraRecord[String, String]


case class TimeoutRecord(destination: String,
                         key: Option[String],
                         payload: String) extends HydraRecord[String, String]