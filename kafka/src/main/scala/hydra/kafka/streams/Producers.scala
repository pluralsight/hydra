package hydra.kafka.streams

import akka.kafka.ProducerSettings
import com.typesafe.config.{Config, ConfigFactory}
import configs.syntax._

object Producers {
  def producerSettings[K, V](id: String, cfg: Config): ProducerSettings[K, V] = {
    val clientConfig = cfg.get[Config](s"hydra.kafka.clients.$id").valueOrElse(ConfigFactory.empty)
    val akkaConfig = cfg.getConfig("akka.kafka.producer")
    ProducerSettings[K, V](clientConfig.atKey("kafka-clients").withFallback(akkaConfig), None, None)
      .withBootstrapServers(cfg.getString("hydra.kafka.producer.bootstrap.servers"))
      .withProperty("client.id", id)
  }
}
