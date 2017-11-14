package hydra.sandbox.transport

import hydra.core.transport.{HydraRecord, RecordMetadata}

/**
  * Created by alexsilva on 3/29/17.
  */
case class FileRecord(destination: String, payload: String)
  extends HydraRecord[String, String] {

  override val key: Option[String] = None
}


case class FileRecordMetadata(path: String, deliveryId: Long = 0L, timestamp: Long = System.currentTimeMillis)
  extends RecordMetadata