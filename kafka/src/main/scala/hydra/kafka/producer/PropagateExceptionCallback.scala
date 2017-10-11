package hydra.kafka.producer

import akka.actor.{ActorRef, ActorSelection}
import hydra.core.transport.{AckStrategy, HydraRecord}
import hydra.core.protocol.{ProducerAck, RecordNotProduced}
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

/**
  * Created by alexsilva on 2/22/17.
  */
case class PropagateExceptionWithAckCallback(producer: ActorSelection,
                                             ingestor: ActorRef,
                                             supervisor: ActorRef,
                                             record: HydraRecord[Any, Any],
                                             ackStrategy: AckStrategy,
                                             deliveryId: Long) extends Callback {

  override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
    if (e != null) {
      producer ! RecordNotProduced(record, e)
    }
    else {
      producer ! KafkaRecordMetadata(metadata, deliveryId, record.deliveryStrategy)
    }

    if (ackStrategy == AckStrategy.Explicit) ingestor ! ProducerAck(supervisor, Option(e))
  }
}