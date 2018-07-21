package hydra.kafka.util

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket

import akka.kafka.{ConsumerSettings, ProducerSettings}
import com.typesafe.config.{Config, ConfigFactory}
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.common.util.TryWith
import hydra.kafka.config.KafkaConfigSupport
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.admin.{AdminClient, CreateTopicsResult, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.concurrent.Future
import scala.util.Try

/**
  * Created by alexsilva on 5/17/17.
  */
case class KafkaUtils(zkString: String, client: () => ZkClient,
                      config: Map[String, AnyRef]) extends LoggingAdapter
  with ConfigSupport {

  private[kafka] var zkUtils = Try(client.apply()).map(ZkUtils(_, false))

  private[kafka] def withRunningZookeeper[T](body: ZkUtils => T): Try[T] = {
    if (zkUtils.isFailure) {
      synchronized {
        zkUtils = Try(client.apply()).map(ZkUtils(_, false))
      }
    }
    zkUtils.map(body)
  }

  def topicExists(name: String): Try[Boolean] = withRunningZookeeper(AdminUtils.topicExists(_, name))

  def topicNames(): Try[Seq[String]] = withRunningZookeeper(_.getAllTopics())

  def createTopic(topic: String, details: TopicDetails, timeout: Int): Future[CreateTopicsResult] = {
    createTopics(Map(topic -> details), timeout)
  }

  def createTopics(topics: Map[String, TopicDetails], timeout: Int): Future[CreateTopicsResult] = {
    Future.fromTry {
      //check for existence first
      withRunningZookeeper { zk =>
        topics.keys.foreach { topic =>
          if (AdminUtils.topicExists(zk, topic)) {
            throw new IllegalArgumentException(s"Topic $topic already exists.")
          }
        }
      }.flatMap { _ => //accounts for topic exists or zookeeper connection error
        val newTopics = topics.map(t =>
          new NewTopic(t._1, t._2.numPartitions, t._2.replicationFactor).configs(t._2.configs))
        TryWith(AdminClient.create(config.asJava)) { client => client.createTopics(newTopics.asJavaCollection) }
      }
    }
  }
}

object KafkaUtils extends ConfigSupport {

  private val _consumerSettings = consumerSettings(rootConfig)

  val stringConsumerSettings: ConsumerSettings[String, String] =
    consumerSettings[String, String]("string", rootConfig)

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


  private[kafka] def requestAndReceive(buffer: Array[Byte], address: String,
                                       port: Int): Try[Array[Byte]] = {
    TryWith(new Socket(address, port)) { socket =>
      val dos = new DataOutputStream(socket.getOutputStream)
      val dis = new DataInputStream(socket.getInputStream)

      dos.writeInt(buffer.length)
      dos.write(buffer)
      dos.flush()
      val resp = new Array[Byte](dis.readInt)
      dis.readFully(resp)
      resp
    }
  }

  def apply(zkString: String, config: Config, connectionTimeout: Int = 5000): KafkaUtils =
    KafkaUtils(zkString, () => new ZkClient(zkString, connectionTimeout),
      ConfigSupport.toMap(config))

  def apply(): KafkaUtils = apply(KafkaConfigSupport.zkString,
    KafkaConfigSupport.kafkaConfig.getConfig("kafka.producer"))
}