package hydra.kafka.transport

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Props, Stash}
import akka.kafka.ProducerSettings
import hydra.common.logging.LoggingAdapter
import hydra.core.protocol._
import hydra.core.transport.TransportCallback
import hydra.kafka.producer.{
  HydraKafkaCallback,
  KafkaRecord,
  KafkaRecordMetadata
}
import hydra.kafka.transport.KafkaProducerProxy.{
  ProduceToKafka,
  ProducerInitializationError
}
import hydra.kafka.transport.KafkaTransport.RecordProduceError
import org.apache.kafka.clients.producer.{Callback, Producer}

import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 10/7/16.
  */
class KafkaProducerProxy[K, V](id: String, settings: ProducerSettings[K, V])
    extends Actor
    with LoggingAdapter
    with Stash {

  private[transport] var producer: Producer[K, V] = _

  implicit val ec = context.dispatcher

  private lazy val selfSel = context.actorSelection(self.path)

  override def receive = initializing

  def initializing: Receive = {
    case _ => stash()
  }

  private def producing: Receive = {
    case ProduceToKafka(deliveryId, kr: KafkaRecord[K, V], ackCallback) =>
      val cb = new HydraKafkaCallback(deliveryId, kr, selfSel, ackCallback)
      produce(kr, cb)

    case kmd: KafkaRecordMetadata =>
      context.parent ! kmd

    case err: RecordProduceError =>
      context.parent ! err
  }

  private def produce(r: KafkaRecord[K, V], callback: Callback) = {
    Try(producer.send(r, callback)).recover {
      case e: Exception =>
        e.printStackTrace()
        callback.onCompletion(null, e)
    }
  }

  private def notInitialized(err: Throwable): Receive = {
    case ProduceToKafka(deliveryId, kr, callback) =>
      context.parent ! RecordProduceError(deliveryId, kr, err)
      callback.onCompletion(deliveryId, None, Some(err))

    case _ =>
      context.parent ! ProducerInitializationError(id, err)
  }

  override def preStart(): Unit = {
    createProducer() match {
      case Success(prod) =>
        log.debug(s"Initialized producer with client id $id")
        producer = prod
        context.become(producing)
        unstashAll()
      case Failure(ex) =>
        log.error(s"Unable to initialize producer with client id $id.", ex)
        context.become(notInitialized(ex))
        unstashAll()
        context.parent ! ProducerInitializationError(id, ex)
    }
  }

  private def createProducer(): Try[Producer[K, V]] =
    Try(settings.createKafkaProducer())

  private def closeProducer(): Try[Unit] = {
    Try {
      Option(producer).foreach { p =>
        p.flush()
        p.close(java.time.Duration.ofSeconds(5))
      }
    }
  }

  override def postStop(): Unit = closeProducer()
}

object KafkaProducerProxy {

  case class ProduceToKafka(
      deliveryId: Long,
      kr: KafkaRecord[_, _],
      callback: TransportCallback
  ) extends HydraMessage

  case class ProducerInitializationError(id: String, ex: Throwable)

  def props[K, V](id: String, settings: ProducerSettings[K, V]): Props =
    Props(classOf[KafkaProducerProxy[K, V]], id, settings)

}

case class InvalidProducerSettingsException(msg: String)
    extends RuntimeException(msg)
