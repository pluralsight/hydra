package hydra.core.transport

/**
  * Created by alexsilva on 2/22/17.
  */
trait RecordMetadata[K, V] {

  def timestamp: Long

  def record: HydraRecord[K, V]

}

case class HydraRecordMetadata[K, V](timestamp: Long, record: HydraRecord[K, V]) extends RecordMetadata[K, V]