package hydra.kafka.producer

import hydra.core.transport.HydraRecord
import org.apache.commons.lang3.ClassUtils
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * Created by alexsilva on 2/22/17.
  */
trait KafkaRecord[K, V] extends HydraRecord[K, V] {
  val timestamp: Long = System.currentTimeMillis()

  def partition: Option[Int] = None

  /**
    * Tries to extract a format name based on the record class name.
    * AvroRecord -> "avro"
    * JsonRecord -> "json"
    * and so forth.
    *
    */
  val formatName: String = {
    val cname = ClassUtils.getSimpleName(getClass)
    val idx = cname.indexOf("Record")
    if (idx != -1) {
      cname.take(idx).toLowerCase
    } else {
      getClass.getName
    }
  }
}

object KafkaRecord {

  implicit def toProducerRecord[K, V](record: KafkaRecord[K, V]): ProducerRecord[K, V] = {
    new ProducerRecord[K, V](
      record.destination,
      record.partition.getOrElse(null).asInstanceOf[Integer],
      record.timestamp,
      record.key,
      record.payload
    )
  }
}
