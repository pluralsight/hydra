package hydra.kafka.util

import akka.kafka.{ConsumerSettings, ProducerSettings}
import com.typesafe.config.{Config, ConfigFactory}
import hydra.common.config.ConfigSupport
import hydra.common.config.ConfigSupport._
import hydra.common.config.KafkaConfigUtils._
import hydra.common.logging.LoggingAdapter
import hydra.common.util.TryWith
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.apache.kafka.clients.admin.{AdminClient, CreateTopicsResult, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Try

case class KafkaUtils(config: Map[String, AnyRef], kafkaClientSecurityConfig: KafkaClientSecurityConfig)
    extends LoggingAdapter
    with ConfigSupport {

  private[kafka] def withClient[T](body: AdminClient => T): Try[T] = {
    TryWith(AdminClient.create((config ++ kafkaClientSecurityConfig.toConfigMap).asJava))(body)
  }

  def topicExists(name: String): Try[Boolean] = withClient { c =>
    c.listTopics().names.get.asScala.exists(s => s == name)
  }

  def topicNames(): Try[Seq[String]] =
    withClient(c => c.listTopics().names.get.asScala.toSeq)

  def createTopic(
      topic: String,
      details: TopicDetails,
      timeout: Int
  ): Future[CreateTopicsResult] = {
    createTopics(Map(topic -> details), timeout)
  }

  def createTopics(
      topics: Map[String, TopicDetails],
      timeout: Int
  ): Future[CreateTopicsResult] = {
    Future.fromTry {
      //check for existence first
      withClient { client =>
        val kafkaTopics = client.listTopics().names().get.asScala
        topics.keys.foreach { topic =>
          if (kafkaTopics.exists(s => s == topic)) {
            throw new IllegalArgumentException(s"Topic $topic already exists.")
          }
        }
      }.flatMap { _ => //accounts for topic exists or zookeeper connection error
        val newTopics = topics.map(t =>
          new NewTopic(t._1, t._2.numPartitions, t._2.replicationFactor)
            .configs(t._2.configs.asJava)
        )
        withClient { client =>
          client.createTopics(newTopics.asJavaCollection)
        }
      }
    }
  }
}

object KafkaUtils extends ConfigSupport {

  final case class TopicDetails(
      numPartitions: Int,
      replicationFactor: Short,
      minInsyncReplicas: Short,
      private val partialConfig: Map[String, String] = Map.empty
  ) {
    val configs: Map[String, String] = partialConfig + ("min.insync.replicas" -> minInsyncReplicas.toString)
  }

  private val _consumerSettings = consumerSettings(rootConfig, defaultKafkaClientSecurityCfg)

  val BootstrapServers: String =
    applicationConfig.getString("kafka.producer.bootstrap.servers")

  val stringConsumerSettings: ConsumerSettings[String, String] =
    consumerSettings[String, String]("string", rootConfig, defaultKafkaClientSecurityCfg)

  def consumerForClientId[K, V](
      clientId: String
  ): Option[ConsumerSettings[K, V]] =
    _consumerSettings.get(clientId).asInstanceOf[Option[ConsumerSettings[K, V]]]

  def loadConsumerSettings[K, V](
      clientId: String,
      groupId: String,
      kafkaClientSecurityConfig: KafkaClientSecurityConfig,
      offsetReset: String = "latest"
  ): ConsumerSettings[K, V] = {
    consumerSettings(rootConfig, kafkaClientSecurityConfig)
      .get(clientId)
      .map(
        _.withGroupId(groupId)
          .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset)
          .asInstanceOf[ConsumerSettings[K, V]]
      )
      .getOrElse(
        throw new IllegalArgumentException(
          s"Id id is not present in any configuration."
        )
      )
  }

  def loadConsumerSettings[K, V](
      cfg: Config,
      kafkaClientSecurityConfig: KafkaClientSecurityConfig,
      groupId: String
  ): ConsumerSettings[K, V] = {
    val akkaConfig =
      rootConfig.getConfig("akka.kafka.consumer").withFallback(cfg)
    val kafkaClientsConfig = cfg.atKey("kafka-clients")
    ConsumerSettings[K, V](
      akkaConfig.withFallback(kafkaClientsConfig),
      None,
      None
    ).withGroupId(groupId)
      .withBootstrapServers(KafkaConfigSupport.bootstrapServers)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
      .withKafkaSecurityConfigs(kafkaClientSecurityConfig)
  }

  def producerSettings[K, V](
      id: String,
      cfg: Config,
      kafkaClientSecurityConfig: KafkaClientSecurityConfig = defaultKafkaClientSecurityCfg
  ): ProducerSettings[K, V] = {
    ProducerSettings[K, V](settingsConfig("producer", id, cfg), None, None)
      .withProperty("client.id", id)
      .withKafkaSecurityConfigs(kafkaClientSecurityConfig)
  }

  def producerSettings(cfg: Config, kafkaClientSecurityConfig: KafkaClientSecurityConfig): Map[String, ProducerSettings[Any, Any]] = {
    val clientsConfig = cfg.getConfig(s"$applicationName.kafka.clients")
    val clients = clientsConfig.root().entrySet().asScala.map(_.getKey)
    clients
      .map(client => client -> producerSettings[Any, Any](client, cfg, kafkaClientSecurityConfig))
      .toMap
  }

  def consumerSettings[K, V](
      id: String,
      cfg: Config,
      kafkaClientSecurityConfig: KafkaClientSecurityConfig
  ): ConsumerSettings[K, V] = {
    ConsumerSettings[K, V](settingsConfig("consumer", id, cfg), None, None)
      .withProperty("client.id", id)
      .withKafkaSecurityConfigs(kafkaClientSecurityConfig)
  }

  def consumerSettings(cfg: Config,
                       kafkaClientSecurityConfig: KafkaClientSecurityConfig = defaultKafkaClientSecurityCfg): Map[String, ConsumerSettings[Any, Any]] = {
    val clientsConfig = cfg.getConfig(s"$applicationName.kafka.clients")
    val clients = clientsConfig.root().entrySet().asScala.map(_.getKey)
    clients
      .map(client => client ->
        consumerSettings[Any, Any](client, cfg, kafkaClientSecurityConfig)
      )
      .toMap
  }

  private def settingsConfig(tpe: String, id: String, cfg: Config): Config = {
    val defaults = cfg.getConfig(s"$applicationName.kafka.$tpe")
    val clientConfig = cfg
      .getConfigOpt(s"$applicationName.kafka.clients.$id.$tpe")
      .getOrElse(ConfigFactory.empty)
      .withFallback(defaults)
    val akkaConfig = cfg.getConfig(s"akka.kafka.$tpe")
    clientConfig.atKey("kafka-clients").withFallback(akkaConfig)
  }

  def apply(config: Config, kafkaSecurityConfig: KafkaClientSecurityConfig): KafkaUtils =
    KafkaUtils(ConfigSupport.toMap(config), kafkaSecurityConfig)

  def apply(kafkaSecurityConfig: KafkaClientSecurityConfig = defaultKafkaClientSecurityCfg): KafkaUtils =
    apply(KafkaConfigSupport.kafkaConfig.getConfig("kafka.admin"), kafkaSecurityConfig)

}
