package hydra.core.transport

/**
  * Created by alexsilva on 2/22/17.
  */
trait RecordMetadata {

  def deliveryId: Long

  def timestamp: Long

}

case class HydraRecordMetadata(deliveryId: Long, timestamp: Long) extends RecordMetadata