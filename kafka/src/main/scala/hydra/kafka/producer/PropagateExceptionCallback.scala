package hydra.kafka.producer

import akka.actor.{ActorRef, ActorSelection}
import hydra.core.produce.HydraRecord
import hydra.core.protocol.{ProducerAck, RecordNotProduced}
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

/**
  * Created by alexsilva on 2/22/17.
  */
case class PropagateExceptionCallback(producer: ActorSelection,
                                      record: HydraRecord[Any, Any]) extends Callback {

  override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
    if (e != null) {
      producer ! RecordNotProduced(record, e)
    }
    else {
      producer ! KafkaRecordMetadata(metadata)
    }
  }
}

case class PropagateExceptionWithAckCallback(producer: ActorSelection,
                                      ingestor: ActorRef,
                                      supervisor: ActorRef,
                                      record: HydraRecord[Any, Any]) extends Callback {

  override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
    if (e != null) {
      producer ! RecordNotProduced(record, e)
      ingestor ! ProducerAck(supervisor, Some(e))
    }
    else {
      producer ! KafkaRecordMetadata(metadata)
      ingestor ! ProducerAck(supervisor, None)
    }
  }
}