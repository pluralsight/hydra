package hydra.kafka.producer

import hydra.core.transport.AckStrategy

/**
  * Created by alexsilva on 10/30/15.
  */
case class DeleteTombstoneRecord(destination: String, key: Option[String], ackStrategy: AckStrategy)
  extends KafkaRecord[String, Any] {

  assert(key.isDefined, "A key is required")

  override val payload = null

  override val formatName = "string" //ok to use any format here
}



