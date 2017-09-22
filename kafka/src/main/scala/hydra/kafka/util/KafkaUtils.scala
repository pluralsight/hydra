package hydra.kafka.util

import akka.kafka.ConsumerSettings
import com.typesafe.config.Config
import hydra.common.logging.LoggingAdapter
import hydra.kafka.config.KafkaConfigSupport
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.collection.immutable.Map
import scala.util.Try

/**
  * Created by alexsilva on 5/17/17.
  */
object KafkaUtils extends KafkaConfigSupport with LoggingAdapter {

  private[kafka] val zkUtils = Try(new ZkClient(zkString, 5000)).map(ZkUtils(_, false))

  def topicExists(name: String): Boolean = {
    zkUtils.map(AdminUtils.topicExists(_, name)) getOrElse {
      log.error("Zookeeper error", zkUtils.failed.get)
      false
    }
  }

  def topicNames(): Try[Seq[String]] = zkUtils.map(_.getAllTopics())

  //bootstrap with the known configs
  val defaultConsumerSettings: Map[String, ConsumerSettings[Any, Any]] = {
    kafkaConsumerFormats.map { case (k, v) =>
      k -> loadConsumerSettings[Any, Any](v, "hydra")
    }
  }

  def loadConsumerSettings[K, V](format: String, groupId: String,
                                 offsetReset: String = "latest"): ConsumerSettings[K, V] = {
    defaultConsumerSettings.get(format)
      .map(_.withGroupId(groupId).withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset)
        .asInstanceOf[ConsumerSettings[K, V]])
      .getOrElse(throw new IllegalArgumentException("Format $format is not present in any configuration."))
  }

  def loadConsumerSettings[K, V](cfg: Config, groupId: String): ConsumerSettings[K, V] = {
    val akkaConfig = rootConfig.getConfig("akka.kafka.consumer").withFallback(cfg)
    val kafkaClientsConfig = cfg.atKey("kafka-clients")
    ConsumerSettings[K, V](akkaConfig.withFallback(kafkaClientsConfig), None, None)
      .withGroupId(groupId)
      .withBootstrapServers(applicationConfig.getString("kafka.producer.bootstrap.servers"))
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
  }

}
