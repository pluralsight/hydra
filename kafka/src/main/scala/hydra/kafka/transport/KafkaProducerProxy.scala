package hydra.kafka.transport

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Props, Stash}
import com.typesafe.config.Config
import hydra.common.logging.LoggingAdapter
import hydra.core.protocol._
import hydra.core.transport.TransportCallback
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.{HydraKafkaCallback, KafkaRecord, KafkaRecordMetadata}
import hydra.kafka.streams.Producers
import hydra.kafka.transport.KafkaProducerProxy.{ProduceToKafka, ProducerInitializationError}
import hydra.kafka.transport.KafkaTransport.RecordProduceError
import org.apache.kafka.clients.producer.{Callback, KafkaProducer}

import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 10/7/16.
  */
class KafkaProducerProxy(id: String, config: Config)
  extends Actor with LoggingAdapter with Stash {

  private[transport] var producer: KafkaProducer[Any, Any] = _

  implicit val ec = context.dispatcher

  private lazy val selfSel = context.actorSelection(self.path)

  override def receive = initializing

  def initializing: Receive = {
    case _ => stash()
  }

  private def producing: Receive = {
    case ProduceToKafka(deliveryId, kr: KafkaRecord[Any, Any], ackCallback) =>
      val cb = new HydraKafkaCallback(deliveryId, kr, selfSel, ackCallback)
      produce(kr, cb)

    case kmd: KafkaRecordMetadata =>
      context.parent ! kmd

    case err: RecordProduceError =>
      context.parent ! err
  }

  private def produce(r: KafkaRecord[Any, Any], callback: Callback) = {
    Try(producer.send(r, callback)).recover {
      case e: Exception =>
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

  private def createProducer(): Try[KafkaProducer[Any, Any]] =
    Try(Producers.producerSettings(id, config).createKafkaProducer())

  private def closeProducer(): Try[Unit] = {
    Try {
      Option(producer).foreach { p =>
        p.flush()
        p.close(5000, TimeUnit.MILLISECONDS)
      }
    }
  }

  override def postStop(): Unit = closeProducer()
}


object KafkaProducerProxy extends KafkaConfigSupport {

  case class ProduceToKafka(deliveryId: Long, kr: KafkaRecord[_, _],
                            callback: TransportCallback) extends HydraMessage

  case class ProducerInitializationError(id: String, ex: Throwable)

  def props(id: String, cfg: Config): Props = Props(classOf[KafkaProducerProxy], id, cfg)


}

case class InvalidProducerSettingsException(msg: String) extends RuntimeException(msg)

