package hydra.kafka.transport

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Props, Stash}
import akka.kafka.ProducerSettings
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.protocol._
import hydra.core.transport.TransportCallback
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.{KafkaRecord, KafkaRecordMetadata, PropagateExceptionWithAckCallback}
import hydra.kafka.transport.KafkaProducerProxy.{ProduceToKafka, ProducerInitializationError}
import hydra.kafka.transport.KafkaTransport.RecordProduceError
import org.apache.kafka.clients.producer.{Callback, KafkaProducer}

import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 10/7/16.
  */
class KafkaProducerProxy(format: String, producerConfig: Config)
  extends Actor with ConfigSupport with LoggingAdapter with Stash {

  private[transport] var producer: KafkaProducer[Any, Any] = _

  implicit val ec = context.dispatcher

  private lazy val selfSel = context.actorSelection(self.path)

  override def receive = initializing

  def initializing: Receive = {
    case _ => stash()
  }

  private def producing: Receive = {
    case ProduceToKafka(deliveryId, kr: KafkaRecord[Any, Any], ackCallback) =>
      val cb = new PropagateExceptionWithAckCallback(deliveryId, kr, selfSel, ackCallback)
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
      context.parent ! ProducerInitializationError(format, err)
  }

  override def preStart(): Unit = {
    createProducer(producerConfig) match {
      case Success(prod) =>
        log.debug(s"Initialized producer with settings $producerConfig")
        producer = prod
        context.become(producing)
        unstashAll()
      case Failure(ex) =>
        log.error(s"Unable to initialize producer with format $format.", ex)
        context.become(notInitialized(ex))
        unstashAll()
        context.parent ! ProducerInitializationError(format, ex)
    }
  }

  private def createProducer(producerConfig: Config): Try[KafkaProducer[Any, Any]] = {
    Try {
      val akkaConfigs = rootConfig.getConfig("akka.kafka.producer")
      val configs = akkaConfigs.withFallback(producerConfig.atKey("kafka-clients"))
      ProducerSettings[Any, Any](configs, None, None).createKafkaProducer()
    }
  }

  private def closeProducer(): Try[Unit] = {
    Try {
      if (producer != null) {
        producer.flush()
        producer.close(5000, TimeUnit.MILLISECONDS)
      }
    }
  }

  override def postStop(): Unit = closeProducer()
}


object KafkaProducerProxy extends KafkaConfigSupport {

  case class ProduceToKafka(deliveryId: Long, kr: KafkaRecord[_, _],
                            callback: TransportCallback) extends HydraMessage

  case class ProducerInitializationError(format: String, ex: Throwable)

  def props(format: String, producerConfig: Config): Props = Props(classOf[KafkaProducerProxy], format, producerConfig)


}

case class InvalidProducerSettingsException(msg: String) extends RuntimeException(msg)

