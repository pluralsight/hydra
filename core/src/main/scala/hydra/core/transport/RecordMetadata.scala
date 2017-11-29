package hydra.core.transport

/**
  * Created by alexsilva on 2/22/17.
  */
trait RecordMetadata {

  def timestamp: Long

}

case class HydraRecordMetadata(timestamp: Long) extends RecordMetadata