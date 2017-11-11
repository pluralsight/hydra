package hydra.kafka.producer

import akka.actor.{ActorRef, ActorSelection}
import hydra.core.protocol
import hydra.core.protocol.RecordNotProduced
import hydra.core.transport.AckStrategy
import hydra.kafka.transport.KafkaTransport.RecordProduceError
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

/**
  * Created by alexsilva on 2/22/17.
  */
case class PropagateExceptionWithAckCallback(deliveryId: Long,
                                             record: KafkaRecord[_, _],
                                             producer: ActorSelection,
                                             ingestor: ActorSelection,
                                             supervisor: ActorRef,
                                             ack: AckStrategy) extends Callback {

  private lazy val shouldAck = ack == AckStrategy.TransportAck

  override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
    Option(e) match {
      case Some(err) => ackError(err)
      case None => doAck(metadata)
    }
  }

  private def doAck(md: RecordMetadata) = {
    val kmd = KafkaRecordMetadata(md, deliveryId)
    producer ! kmd
    if (shouldAck) ingestor ! protocol.RecordProduced(kmd, supervisor)
  }

  private def ackError(e: Exception) = {
    producer ! RecordProduceError(deliveryId, record, e)
    if (shouldAck) ingestor ! RecordNotProduced(deliveryId, record, e, supervisor)
  }
}