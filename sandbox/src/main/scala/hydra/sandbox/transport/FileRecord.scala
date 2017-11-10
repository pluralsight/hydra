package hydra.sandbox.transport

import hydra.core.transport.{AckStrategy, DeliveryStrategy, HydraRecord, RecordMetadata}

/**
  * Created by alexsilva on 3/29/17.
  */
case class FileRecord(destination: String, payload: String, ackStrategy: AckStrategy = AckStrategy.None)
  extends HydraRecord[String, String] {

  override val key: Option[String] = None

  override val deliveryStrategy: DeliveryStrategy = DeliveryStrategy.AtMostOnce
}


case class FileRecordMetadata(path: String, deliveryId: Long = 0L) extends RecordMetadata