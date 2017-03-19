package hydra.kafka.akka

import akka.kafka.ConsumerSettings
import hydra.kafka.config.KafkaConfigSupport
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.concurrent.duration._
import scala.util.{Failure, Try}
import scalacache.guava.GuavaCache
import scalacache.{ScalaCache, sync}

/**
  * Created by alexsilva on 3/18/17.
  */
object ConsumerSettingsHelper extends KafkaConfigSupport {

  private implicit val cache = ScalaCache(GuavaCache())

  def loadConsumerSettings[K, V](topic: String, format: String, groupId: String): Try[ConsumerSettings[K, V]] = {
    sync.cachingWithTTL[Try[ConsumerSettings[K, V]]](s"$topic.$format")(5.minutes) {
      kafkaConsumerFormats.get(format).map { cfg =>
        val akkaConfig = rootConfig.getConfig("akka.kafka.consumer").withFallback(cfg)
        val kafkaClientsConfig = cfg.atKey("kafka-clients")
        Try(ConsumerSettings[K, V](akkaConfig.withFallback(kafkaClientsConfig), None, None)
          .withGroupId(groupId)
          .withBootstrapServers(applicationConfig.getString("kafka.producer.bootstrap.servers"))
          .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"))
      }.getOrElse(Failure(new IllegalArgumentException(s"No configuration for format $format found.")))
    }
  }
}