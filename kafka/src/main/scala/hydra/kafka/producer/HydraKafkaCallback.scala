package hydra.kafka.producer

import akka.actor.ActorSelection
import hydra.core.monitor.HydraMetrics
import hydra.core.transport.TransportCallback
import hydra.kafka.transport.KafkaTransport.RecordProduceError
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

/**
  * Created by alexsilva on 2/22/17.
  */
case class HydraKafkaCallback(deliveryId: Long,
                              record: KafkaRecord[_, _],
                              producer: ActorSelection,
                              callback: TransportCallback) extends Callback {

  override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
    Option(e) match {
      case Some(err) => ackError(err)
      case None => doAck(metadata)
    }
  }

  private def doAck(md: RecordMetadata) = {
    val kmd = KafkaRecordMetadata(md, deliveryId)
    HydraMetrics.countSuccess("hydra_ingest_records_published_total", md.topic())
    producer ! kmd
    callback.onCompletion(deliveryId, Some(kmd), None)
  }

  private def ackError(e: Exception) = {
    HydraMetrics.countFail("hydra_ingest_records_published_total", record.destination)
    producer ! RecordProduceError(deliveryId, record, e)
    callback.onCompletion(deliveryId, None, Some(e))
  }
}