package hydra.kafka.producer

import akka.kafka.ProducerSettings
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport

/**
  * Created by alexsilva on 3/25/17.
  */
object KafkaProducers extends ConfigSupport {

  def createProducer(producerConfig: Config) = {
    val akkaConfigs = rootConfig.getConfig("akka.kafka.producer")
    val configs = akkaConfigs.withFallback(producerConfig.atKey("kafka-clients"))
    ProducerSettings[Any, Any](configs, None, None).createKafkaProducer()
  }
}
