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

class PublishMetrics(topic: String)(implicit system: ActorSystem)
    extends KafkaMetrics
    with DefaultJsonProtocol
    with ConfigSupport {

  import spray.json._

  import KafkaRecordMetadata._

  private val producer = KafkaUtils
    .producerSettings[String, String]("string", rootConfig)
    .withProperty("client.id", "hydra.kafka.metrics")
    .createKafkaProducer()

  def saveMetrics(record: KafkaRecordMetadata) = {
    val payload = record.toJson.compactPrint
    producer.send(new ProducerRecord(topic, record.destination, payload))
  }

  override def close(): Unit = {
    producer.close()
  }

}

object KafkaMetrics {

  import ConfigSupport._

  def apply(config: Config)(implicit system: ActorSystem): KafkaMetrics = {
    val metricsEnabled =
      config.getBooleanOpt("transports.kafka.metrics.enabled").getOrElse(false)

    val metricsTopic = config
      .getStringOpt("transports.kafka.metrics.topic")
      .getOrElse("HydraKafkaError")

    if (metricsEnabled) new PublishMetrics(metricsTopic) else NoOpMetrics
  }
}
