package hydra.core.test

import hydra.core.ingest.HydraRequest
import hydra.core.transport._

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


case class TestRecordMetadata(deliveryId: Long, deliveryStrategy: DeliveryStrategy = DeliveryStrategy.AtMostOnce)
  extends RecordMetadata

case class TimeoutRecord(destination: String,
                         key: Option[String],
                         payload: String,
                         deliveryStrategy: DeliveryStrategy = DeliveryStrategy.AtLeastOnce,
                         ackStrategy: AckStrategy = AckStrategy.None) extends HydraRecord[String, String]