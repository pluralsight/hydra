package hydra.kafka.transport

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Props, Stash}
import akka.kafka.ProducerSettings
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.protocol.{RecordNotProduced, RecordProduced}
import hydra.core.transport.HydraRecord
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.{KafkaRecord, KafkaRecordMetadata, PropagateExceptionCallback, PropagateExceptionWithAckCallback}
import hydra.kafka.transport.KafkaProducerProxy.{ProduceToKafka, ProduceToKafkaWithAck, ProducerInitializationError}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer}

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
    case ProduceToKafka(r: KafkaRecord[Any, Any], deliveryId) =>
      produce(r, new PropagateExceptionCallback(context.actorSelection(self.path), r, deliveryId))

    case ProduceToKafkaWithAck(r: KafkaRecord[Any, Any], ingestor, supervisor, deliveryId) =>
      produce(r,
        new PropagateExceptionWithAckCallback(context.actorSelection(self.path), ingestor, supervisor, r, deliveryId))

    case kmd: KafkaRecordMetadata =>
      context.parent ! RecordProduced(kmd)

    case err: RecordNotProduced[_, _] =>
      context.parent ! err
      throw err.error
  }

  private def produce(r: KafkaRecord[Any, Any], callback: Callback) = {
    Try(producer.send(r, callback)).recover {
      case e: Exception =>
        callback.onCompletion(null, e)
    }
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
        context.become {
          case _ => context.parent ! ProducerInitializationError(format, ex)
        }
        unstashAll()
        println("RAR"+context.parent.path)
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

  case class ProduceToKafka[K, V](r: KafkaRecord[K, V], deliveryId: Long)

  case class ProduceToKafkaWithAck[K, V](record: HydraRecord[K, V], ingestor: ActorRef, supervisor: ActorRef,
                                         deliveryId: Long)

}

case class InvalidProducerSettingsException(msg: String) extends RuntimeException(msg)

