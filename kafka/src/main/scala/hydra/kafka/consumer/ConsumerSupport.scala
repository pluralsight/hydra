package hydra.kafka.consumer

import akka.kafka.{ConsumerSettings, Subscription, Subscriptions}
import com.typesafe.config.Config
import hydra.kafka.config.KafkaConfigSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by alexsilva on 3/20/17.
  */
trait ConsumerSupport extends KafkaConfigSupport {

  private val defaultSettings = loadConsumerSettings[String, String](kafkaConsumerDefaults, "hydra")

  val defaultConsumer = defaultSettings.createKafkaConsumer()

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

  def latestOffsets(topic: String)(implicit ec: ExecutionContext): Future[Map[TopicPartition, Long]] = {
    Future[Map[TopicPartition, Long]] {
      val ts = defaultConsumer.partitionsFor(topic).asScala.map(pi => new TopicPartition(topic, pi.partition()))
      defaultConsumer.endOffsets(ts.asJava).asScala.map(tp => tp._1 -> tp._2.toLong).toMap
    }
  }

  implicit def toSubscription(partitions: Seq[PartitionInfo]): Subscription = {
    val tps = partitions.map(p => new TopicPartition(p.topic(), p.partition()))
    Subscriptions.assignment(tps: _*)
  }

}
