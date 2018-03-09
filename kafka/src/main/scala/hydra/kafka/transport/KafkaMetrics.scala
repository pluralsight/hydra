package hydra.kafka.transport

import akka.actor.ActorSystem
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.kafka.producer.KafkaRecordMetadata
import hydra.kafka.util.KafkaUtils
import org.apache.kafka.clients.producer.ProducerRecord
import spray.json.DefaultJsonProtocol

trait KafkaMetrics {
  def saveMetrics(record: KafkaRecordMetadata): Unit

  def close(): Unit = {}
}

// $COVERAGE-OFF$
object NoOpMetrics extends KafkaMetrics {
  def saveMetrics(record: KafkaRecordMetadata): Unit = {}
}

// $COVERAGE-ON$

class PublishMetrics(topic: String)(implicit system: ActorSystem) extends KafkaMetrics
  with DefaultJsonProtocol
  with ConfigSupport {

  import spray.json._

  private implicit val mdFormat = jsonFormat5(KafkaRecordMetadata.apply)

  private val producer = {
    KafkaUtils.producerSettings[String, String]("string", rootConfig)
      .withProperty("client.id", "hydra.kafka.metrics")
      .createKafkaProducer()
  }

  def saveMetrics(record: KafkaRecordMetadata) = {
    val payload = record.toJson.compactPrint
    producer.send(new ProducerRecord(topic, record.topic, payload))
  }

  override def close(): Unit = {
    producer.close()
  }

}

object KafkaMetrics {

  import configs.syntax._

  def apply(config: Config)(implicit system: ActorSystem): KafkaMetrics = {
    val metricsEnabled = config.get[Boolean]("transports.kafka.metrics.enabled").valueOrElse(false)

    val metricsTopic = config.get[String]("transports.kafka.metrics.topic").valueOrElse("HydraKafkaError")

    if (metricsEnabled) new PublishMetrics(metricsTopic) else NoOpMetrics
  }
}