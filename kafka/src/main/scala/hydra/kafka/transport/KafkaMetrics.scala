package hydra.kafka.transport

import akka.actor.ActorSystem
import com.typesafe.config.Config
import hydra.kafka.producer.{JsonRecord, KafkaRecordMetadata}
import hydra.kafka.transport.KafkaTransport.ProduceOnly

trait KafkaMetrics {
  def saveMetrics(record: KafkaRecordMetadata): Unit
}

object NoOpMetrics extends KafkaMetrics {
  def saveMetrics(record: KafkaRecordMetadata): Unit = {}
}

class PublishMetrics(producerPath: String, topic: String)(implicit system: ActorSystem) extends KafkaMetrics {
  private lazy val producer = system.actorSelection(producerPath)

  def saveMetrics(record: KafkaRecordMetadata) = {
    producer ! ProduceOnly(JsonRecord(topic, Some(record.topic), record))
  }
}

object KafkaMetrics {

  import configs.syntax._

  def apply(config: Config)(implicit system: ActorSystem): KafkaMetrics = {
    val metricsEnabled = config.get[Boolean]("producers.kafka.metrics.enabled").valueOrElse(false)

    val metricsTopic = config.get[String]("producers.kafka.metrics.topic").valueOrElse("HydraKafkaError")

    val metricsActor = config.get[String]("producers.kafka.metrics.actor_path")
      .valueOrElse("kafka_transport/json")

    if (metricsEnabled) new PublishMetrics(metricsActor, metricsTopic) else NoOpMetrics
  }
}