package hydra.kafka.producer

import akka.actor.{ActorRef, ActorSelection}
import hydra.core.protocol
import hydra.core.protocol.RecordNotProduced
import hydra.kafka.transport.KafkaTransport.RecordProduceError
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

/**
  * Created by alexsilva on 2/22/17.
  */
case class PropagateExceptionWithAckCallback(deliveryId: Long,
                                             record: KafkaRecord[_, _],
                                             producer: ActorSelection,
                                             ingestionActors: Option[(ActorSelection, ActorRef)]) extends Callback {

  override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
    Option(e) match {
      case Some(err) => ackError(err)
      case None => doAck(metadata)
    }
  }

  private def doAck(md: RecordMetadata) = {
    val kmd = KafkaRecordMetadata(md, deliveryId)
    producer ! kmd
    ingestionActors.foreach(a => a._1 ! protocol.RecordProduced(kmd, a._2))
  }

  private def ackError(e: Exception) = {
    producer ! RecordProduceError(deliveryId, record, e)
    ingestionActors.foreach(a => a._1 ! RecordNotProduced(deliveryId, record, e, a._2))
  }
}