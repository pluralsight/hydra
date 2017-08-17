package hydra.kafka.producer

import hydra.core.transport.{RecordMetadata, RetryStrategy}

/**
  * Created by alexsilva on 2/22/17.
  */
case class KafkaRecordMetadata(offset: Long, timestamp: Long, topic: String, partition: Int,
                               deliveryId: Long, retryStrategy: RetryStrategy) extends RecordMetadata

object KafkaRecordMetadata {
  def apply(kmd: org.apache.kafka.clients.producer.RecordMetadata,
            deliveryId: Long, rt: RetryStrategy): KafkaRecordMetadata = {
    KafkaRecordMetadata(kmd.offset(), kmd.timestamp(), kmd.topic(), kmd.partition(), deliveryId, rt)
  }
}