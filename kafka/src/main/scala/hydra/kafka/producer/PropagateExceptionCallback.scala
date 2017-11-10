package hydra.kafka.producer

import akka.actor.{ActorRef, ActorSelection}
import hydra.core.protocol.{RecordNotProduced, RecordProduced}
import hydra.core.transport.{AckStrategy, HydraRecord}
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

  val shouldAck = ackStrategy == AckStrategy.Explicit

  override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
    if (e != null) {
      producer ! RecordNotProduced(record, e)
      if (shouldAck) ingestor ! RecordNotProduced(record, e, Some(supervisor))
    }
    else {
      val kmd = KafkaRecordMetadata(metadata, deliveryId)
      producer ! kmd
      if (shouldAck) ingestor ! RecordProduced(kmd, Some(supervisor))
    }


  }
}