package hydra.sandbox.transport

import hydra.core.transport.{AckStrategy, DeliveryStrategy, HydraRecord, RecordMetadata}

/**
  * Created by alexsilva on 3/29/17.
  */
case class FileRecord(destination: String, payload: String) extends HydraRecord[String, String] {

  override val key: Option[String] = None

  override val deliveryStrategy: DeliveryStrategy = DeliveryStrategy.AtMostOnce
  override val ackStrategy: AckStrategy = AckStrategy.None
}


case class FileRecordMetadata(path: String, deliveryId: Long = 0L,
                              retryStrategy: DeliveryStrategy) extends RecordMetadata