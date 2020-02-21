package hydra.sandbox.transport

import hydra.core.transport.{AckStrategy, HydraRecord, RecordMetadata}
import org.apache.commons.lang3.StringUtils

/**
  * Created by alexsilva on 3/29/17.
  */
case class FileRecord(
    destination: String,
    payload: String,
    ackStrategy: AckStrategy
) extends HydraRecord[String, String] {

  override val key = StringUtils.EMPTY //not needed
}

case class FileRecordMetadata(
    destination: String,
    deliveryId: Long = 0L,
    timestamp: Long = System.currentTimeMillis,
    ackStrategy: AckStrategy
) extends RecordMetadata
