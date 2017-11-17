package hydra.kafka.producer

import akka.actor.ActorSelection
import hydra.core.transport.Transport
import hydra.kafka.transport.KafkaTransport.RecordProduceError
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

/**
  * Created by alexsilva on 2/22/17.
  */
case class PropagateExceptionWithAckCallback(deliveryId: Long,
                                             record: KafkaRecord[_, _],
                                             producer: ActorSelection,
                                             ackCallback: Transport.AckCallback) extends Callback {

  override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
    Option(e) match {
      case Some(err) => ackError(err)
      case None => doAck(metadata)
    }
  }

  private def doAck(md: RecordMetadata) = {
    val kmd = KafkaRecordMetadata(md, deliveryId)
    producer ! kmd
    ackCallback(Some(kmd), None)
  }

  private def ackError(e: Exception) = {
    producer ! RecordProduceError(deliveryId, record, e)
    ackCallback(None, Some(e))
  }
}