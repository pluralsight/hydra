package hydra.kafka.transport

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Props, Stash}
import akka.kafka.ProducerSettings
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.protocol._
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.{KafkaRecord, KafkaRecordMetadata, PropagateExceptionWithAckCallback}
import hydra.kafka.transport.KafkaProducerProxy.{ProduceOnly, ProducerInitializationError}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, RecordMetadata}

import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 10/7/16.
  */
class KafkaProducerProxy(format: String, producerConfig: Config)
  extends Actor with ConfigSupport with LoggingAdapter with Stash {

  private[transport] var producer: KafkaProducer[Any, Any] = _

  implicit val ec = context.dispatcher

  override def receive = initializing

  def initializing: Receive = {
    case _ => stash()
  }

  private def producing: Receive = {
    case Produce(r: KafkaRecord[Any, Any], ingestor, supervisor, deliveryId) =>
      val p = context.actorSelection(self.path)
      val cb = new PropagateExceptionWithAckCallback(p, ingestor, supervisor, r, r.ackStrategy, deliveryId)
      produce(r, cb)

    case ProduceOnly(kr: KafkaRecord[Any, Any]) =>
      producer.send(kr, (metadata: RecordMetadata, e: Exception) => {
        val msg = if (e != null) RecordNotProduced(kr, e) else KafkaRecordMetadata(metadata, 0, kr.deliveryStrategy)
        self ! msg
      })

    case kmd: KafkaRecordMetadata =>
      context.parent ! RecordProduced(kmd)

    case err: RecordNotProduced[_, _] =>
      context.parent ! err
  }

  private def produce(r: KafkaRecord[Any, Any], callback: Callback) = {
    Try(producer.send(r, callback)).recover {
      case e: Exception =>
        callback.onCompletion(null, e)
    }
  }

  private def notInitialized(err: Throwable): Receive = {
    case Produce(r: KafkaRecord[Any, Any], ingestor, supervisor, _) =>
      context.parent ! RecordNotProduced(r, err)
      ingestor ! ProducerAck(supervisor, Some(err))

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
        producer.close(10000, TimeUnit.MILLISECONDS)
      }
    }
  }

  override def postStop(): Unit = closeProducer()
}


object KafkaProducerProxy extends KafkaConfigSupport {

  case class ProducerInitializationError(format: String, ex: Throwable)

  def props(format: String, producerConfig: Config): Props = Props(classOf[KafkaProducerProxy], format, producerConfig)

  /**
    * Sends a message to kafka without having to track ingestor and supervisors.
    * No acknowledgment logic is provided either.
    *
    * @param kafkaRecord
    */
  case class ProduceOnly[K, V](kafkaRecord: KafkaRecord[K, V])

}

case class InvalidProducerSettingsException(msg: String) extends RuntimeException(msg)

