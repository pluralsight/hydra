package hydra.kafka.transport

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Props, Stash}
import akka.kafka.ProducerSettings
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.protocol.{Produce, RecordNotProduced, RecordProduced}
import hydra.core.transport.HydraRecord
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.{KafkaRecord, KafkaRecordMetadata, PropagateExceptionCallback, PropagateExceptionWithAckCallback}
import hydra.kafka.transport.KafkaProducerProxy.{ProduceToKafka, ProduceToKafkaWithAck, ProducerInitialized}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 10/7/16.
  */
class KafkaProducerProxy(parent: ActorRef, producerConfig: Config) extends Actor
  with ConfigSupport with LoggingAdapter with Stash {

  /**
    * We make this a var so that it can be re-assigned during preRestart() if needed.
    */
  private[transport] var producer: KafkaProducer[Any, Any] = _

  implicit val ec = context.dispatcher

  def receive = initializing

  def initializing: Receive = {
    case ProducerInitialized =>
      context.become(receiving)
      unstashAll()
    case _ => stash()
  }

  private def produce(r: KafkaRecord[Any, Any], callback: Callback) = {
    Try(producer.send(r, callback)).recover {
      case e: Exception =>
        callback.onCompletion(null, e)
    }
  }

  def receiving: Receive = {
    case ProduceToKafka(r: KafkaRecord[Any, Any], deliveryId) =>
      produce(r, new PropagateExceptionCallback(context.actorSelection(self.path), r, deliveryId))

    case ProduceToKafkaWithAck(r: KafkaRecord[Any, Any], ingestor, supervisor, deliveryId) =>
      produce(r,
        new PropagateExceptionWithAckCallback(context.actorSelection(self.path), ingestor, supervisor, r, deliveryId))

    case kmd: KafkaRecordMetadata =>
      parent ! RecordProduced(kmd)

    case err: RecordNotProduced[_, _] =>
      parent ! err
      throw err.error
  }

  def initProducer(): Unit = {
    Future(createProducer()) onComplete {
      case Success(prod) =>
        log.debug(s"Initialized producer with settings $producerConfig")
        producer = prod
        self ! ProducerInitialized
      case Failure(ex) =>
        log.error("Unable to initialize producer.", ex)
        throw new InvalidProducerSettingsException(ex.getMessage)
    }
  }

  override def preStart(): Unit = {
    initProducer()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    initProducer()
    message collect {
      //try sending it again
      case Some(msg: RecordNotProduced[_, _]) => self ! Produce(msg.record)
    }
  }

  private def createProducer(): KafkaProducer[Any, Any] = {
    val akkaConfigs = rootConfig.getConfig("akka.kafka.producer")
    val configs = akkaConfigs.withFallback(producerConfig.atKey("kafka-clients"))
    ProducerSettings[Any, Any](configs, None, None).createKafkaProducer()
  }

  override def postStop(): Unit = {
    producer.flush()
    producer.close(10000, TimeUnit.MILLISECONDS)
  }
}

object KafkaProducerProxy extends KafkaConfigSupport {

  def props(parent: ActorRef, producerConfig: Config): Props =
    Props(classOf[KafkaProducerProxy], parent, producerConfig)

  def props(parent: ActorRef, format: String): Props = {
    kafkaProducerFormats.get(format) match {
      case Some(config) => Props(classOf[KafkaProducerProxy], parent, config)
      case None => throw new InvalidProducerSettingsException(s"No configuration found for format '$format'.")
    }
  }

  case object ProducerInitialized

  case class ProduceToKafka[K, V](r: KafkaRecord[K, V], deliveryId: Long)

  case class ProduceToKafkaWithAck[K, V](record: HydraRecord[K, V], ingestor: ActorRef, supervisor: ActorRef,
                                         deliveryId: Long)

}

case class InvalidProducerSettingsException(msg: String) extends RuntimeException(msg)

