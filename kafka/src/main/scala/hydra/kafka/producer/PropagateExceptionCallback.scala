package hydra.kafka.producer

import akka.actor.ActorSelection
import hydra.core.producer.HydraRecord
import hydra.core.protocol.RecordNotProduced
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

/**
  * Created by alexsilva on 2/22/17.
  */
case class PropagateExceptionCallback(ref: ActorSelection, record: HydraRecord[Any, Any]) extends Callback {
  override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
    if (e != null)
      ref ! RecordNotProduced(record, e)
    else
      ref ! KafkaRecordMetadata(metadata)
  }
}