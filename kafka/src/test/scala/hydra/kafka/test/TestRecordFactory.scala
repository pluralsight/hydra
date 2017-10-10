package hydra.kafka.test

import hydra.core.ingest.HydraRequest
import hydra.core.transport.{AckStrategy, DeliveryStrategy, HydraRecord, RecordFactory}

import scala.util.Success

object TestRecordFactory extends RecordFactory[String, String] {
  override def build(r: HydraRequest) = {
    val timeout = r.metadataValueEquals("timeout", "true")
    if (timeout) {
      Success(TimeoutRecord("test-topic", Some(r.correlationId.toString), r.payload))
    }
    else {
      Success(TestRecord("test-topic", Some(r.correlationId.toString), r.payload))
    }
  }
}

case class TestRecord(destination: String,
                      key: Option[String],
                      payload: String,
                      deliveryStrategy: DeliveryStrategy = DeliveryStrategy.AtLeastOnce,
                      ackStrategy: AckStrategy = AckStrategy.None) extends HydraRecord[String, String]


case class TimeoutRecord(destination: String,
                         key: Option[String],
                         payload: String,
                         deliveryStrategy: DeliveryStrategy = DeliveryStrategy.AtLeastOnce,
                         ackStrategy: AckStrategy = AckStrategy.None) extends HydraRecord[String, String]