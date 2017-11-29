package hydra.kafka.transport

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import com.typesafe.config.Config
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.producer.KafkaRecordMetadata
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import spray.json.DefaultJsonProtocol

trait KafkaMetrics {
  def saveMetrics(record: KafkaRecordMetadata): Unit
}

object NoOpMetrics extends KafkaMetrics {
  def saveMetrics(record: KafkaRecordMetadata): Unit = {}
}

class PublishMetrics(topic: String)(implicit system: ActorSystem) extends KafkaMetrics with KafkaConfigSupport
  with DefaultJsonProtocol {

  import spray.json._

  private implicit val mdFormat = jsonFormat5(KafkaRecordMetadata.apply)

  private val producer = {
    val akkaConfigs = rootConfig.getConfig("akka.kafka.producer")
    val configs = akkaConfigs.withFallback(kafkaProducerFormats("string").atKey("kafka-clients"))
    ProducerSettings[String, String](configs, None, None).withProperty("client.id", "hydra.kafka.metrics")
      .createKafkaProducer()
  }

  def saveMetrics(record: KafkaRecordMetadata) = {
    val payload = record.toJson.compactPrint
    producer.send(new ProducerRecord(topic, record.topic, payload), new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        println(metadata)
        println(exception)
      }
    })
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