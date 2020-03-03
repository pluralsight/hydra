package hydra.core.transport

/**
  * Created by alexsilva on 2/22/17.
  */
trait RecordMetadata {

  def timestamp: Long

  def destination: String

  def ackStrategy: AckStrategy

}

case class HydraRecordMetadata(
    timestamp: Long,
    destination: String,
    ackStrategy: AckStrategy
) extends RecordMetadata
