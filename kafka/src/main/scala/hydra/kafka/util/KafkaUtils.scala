package hydra.kafka.util

import akka.kafka.{ConsumerSettings, ProducerSettings}
import com.typesafe.config.{Config, ConfigFactory}
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.kafka.config.KafkaConfigSupport
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.util.Try

/**
  * Created by alexsilva on 5/17/17.
  */
object KafkaUtils extends LoggingAdapter with ConfigSupport {

  import KafkaConfigSupport._

  private[kafka] var zkUtils = Try(new ZkClient(zkString, 5000)).map(ZkUtils(_, false))

  private[kafka] def withRunningZookeeper[T](body: ZkUtils => T): Try[T] = {
    if (zkUtils.isFailure) {
      synchronized {
        zkUtils = Try(new ZkClient(zkString, 5000)).map(ZkUtils(_, false))
      }
    }
    zkUtils.map(body)
  }

  private val _consumerSettings = consumerSettings(rootConfig)

  val stringConsumerSettings: ConsumerSettings[String, String] =
    consumerSettings[String, String]("string", rootConfig)

  def topicExists(name: String): Try[Boolean] = withRunningZookeeper(AdminUtils.topicExists(_, name))

  def topicNames(): Try[Seq[String]] = withRunningZookeeper(_.getAllTopics())

  def consumerForClientId[K, V](clientId: String): Option[ConsumerSettings[K, V]] =
    _consumerSettings.get(clientId).asInstanceOf[Option[ConsumerSettings[K, V]]]

  def loadConsumerSettings[K, V](clientId: String, groupId: String,
                                 offsetReset: String = "latest"): ConsumerSettings[K, V] = {
    _consumerSettings.get(clientId)
      .map(_.withGroupId(groupId).withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset)
        .asInstanceOf[ConsumerSettings[K, V]])
      .getOrElse(throw new IllegalArgumentException(s"Id id is not present in any configuration."))
  }

  def loadConsumerSettings[K, V](cfg: Config, groupId: String): ConsumerSettings[K, V] = {
    val akkaConfig = rootConfig.getConfig("akka.kafka.consumer").withFallback(cfg)
    val kafkaClientsConfig = cfg.atKey("kafka-clients")
    ConsumerSettings[K, V](akkaConfig.withFallback(kafkaClientsConfig), None, None)
      .withGroupId(groupId)
      .withBootstrapServers(KafkaConfigSupport.bootstrapServers)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
  }


  def producerSettings[K, V](id: String, cfg: Config): ProducerSettings[K, V] = {
    ProducerSettings[K, V](settingsConfig("producer", id, cfg), None, None)
      .withProperty("client.id", id)
  }

  def producerSettings(cfg: Config): Map[String, ProducerSettings[Any, Any]] = {
    val clientsConfig = cfg.getConfig(s"$applicationName.kafka.clients")
    val clients = clientsConfig.root().entrySet().asScala.map(_.getKey)
    clients.map(client => client -> producerSettings[Any, Any](client, cfg)).toMap
  }

  def consumerSettings[K, V](id: String, cfg: Config): ConsumerSettings[K, V] = {
    ConsumerSettings[K, V](settingsConfig("consumer", id, cfg), None, None)
      .withProperty("client.id", id)
  }

  def consumerSettings(cfg: Config): Map[String, ConsumerSettings[Any, Any]] = {
    val clientsConfig = cfg.getConfig(s"$applicationName.kafka.clients")
    val clients = clientsConfig.root().entrySet().asScala.map(_.getKey)
    clients.map(client => client -> consumerSettings[Any, Any](client, cfg)).toMap
  }

  private def settingsConfig(tpe: String, id: String, cfg: Config): Config = {
    val defaults = cfg.getConfig(s"$applicationName.kafka.$tpe")
    val clientConfig = cfg.get[Config](s"$applicationName.kafka.clients.$id.$tpe").
      valueOrElse(ConfigFactory.empty).withFallback(defaults)
    val akkaConfig = cfg.getConfig(s"akka.kafka.$tpe")
    clientConfig.atKey("kafka-clients").withFallback(akkaConfig)
  }
}
