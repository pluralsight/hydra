package hydra.kafka.producer

import hydra.core.transport.RecordMetadata

/**
  * Created by alexsilva on 2/22/17.
  */
case class KafkaRecordMetadata(offset: Long, timestamp: Long, topic: String, partition: Int) extends RecordMetadata

object KafkaRecordMetadata {
  def apply(kmd: org.apache.kafka.clients.producer.RecordMetadata): KafkaRecordMetadata = {
    KafkaRecordMetadata(kmd.offset(), kmd.timestamp(), kmd.topic(), kmd.partition())
  }
}