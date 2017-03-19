package hydra.kafka.producer

import hydra.core.produce.HydraRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.util.ClassUtils

/**
  * Created by alexsilva on 2/22/17.
  */
trait KafkaRecord[K, V] extends HydraRecord[K, V] {
  val timestamp = System.currentTimeMillis()

  def partition: Option[Int] = None

  /**
    * Tries to extract a format name based on the record class name.
    * AvroRecord -> "avro"
    * JsonRecord -> "json"
    * and so forth.
    *
    */
  val formatName: String = {
    val cname = ClassUtils.getShortName(getClass)
    val idx = cname.indexOf("Record")
    if (idx != -1)
      cname.take(idx).toLowerCase
    else
      getClass.getName
  }
}

object KafkaRecord {
  implicit def toProducerRecord[K, V](record: KafkaRecord[K, V]) = {
    new ProducerRecord[K, V](record.destination,
      record.partition.getOrElse(null).asInstanceOf[Integer],
      record.timestamp,
      record.key.getOrElse(null).asInstanceOf[K],
      record.payload)
  }
}
