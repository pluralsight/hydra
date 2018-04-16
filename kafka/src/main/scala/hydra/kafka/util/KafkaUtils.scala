package hydra.kafka.util

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.ByteBuffer

import akka.kafka.{ConsumerSettings, ProducerSettings}
import com.typesafe.config.{Config, ConfigFactory}
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.common.util.TryWith
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.util.KafkaUtils.requestAndReceive
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails
import org.apache.kafka.common.requests.{CreateTopicsRequest, CreateTopicsResponse, RequestHeader, ResponseHeader}

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.util.{Random, Try}

/**
  * Created by alexsilva on 5/17/17.
  */
case class KafkaUtils(zkString: String, client: () => ZkClient) extends LoggingAdapter
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

  def createTopic(topic: String, details: TopicDetails, timeout: Int): Try[CreateTopicsResponse] = {
    createTopics(Map(topic -> details), timeout)
  }

  def createTopics(topics: Map[String, TopicDetails], timeout: Int): Try[CreateTopicsResponse] = {
    //check for existence first
    withRunningZookeeper { zk =>
      topics.keys.foreach { topic =>
        if (AdminUtils.topicExists(zk, topic)) {
          throw new IllegalArgumentException(s"Topic $topic already exist.")
        }
      }
    }.flatMap { _ => //accounts for topic exists or zookeeper connection error
      val builder = new CreateTopicsRequest.Builder(topics.asJava, timeout, false)
      val broker = Random.shuffle(KafkaConfigSupport.bootstrapServers.split(",").toSeq).head
      createTopicResponse(builder.build(), broker)
    }
  }

  private def createTopicResponse(request: CreateTopicsRequest,
                                  brokerInfo: String): Try[CreateTopicsResponse] = {
    val comp = brokerInfo.split(":")
    require(comp.length == 2, s"$comp is not a valid broker address. Use [host:port].")
    val address = comp(0)
    val port = comp(1).toInt

    val header = new RequestHeader(ApiKeys.CREATE_TOPICS.id, 1, brokerInfo, -1)
    val buffer = ByteBuffer.allocate(header.sizeOf + request.sizeOf)
    header.writeTo(buffer)
    request.writeTo(buffer)
    requestAndReceive(buffer.array, address, port).map { resp =>
      val respBuffer = ByteBuffer.wrap(resp)
      ResponseHeader.parse(respBuffer)
      CreateTopicsResponse.parse(respBuffer)
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
      new Array[Byte](dis.readInt)
    }
  }

  def apply(zkString: String, connectionTimeout: Int = 5000): KafkaUtils =
    KafkaUtils(zkString, () => new ZkClient(zkString, connectionTimeout))

  def apply(): KafkaUtils = apply(KafkaConfigSupport.zkString)
}