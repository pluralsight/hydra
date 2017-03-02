package hydra.kafka.health

import akka.actor.Actor
import com.github.vonnagy.service.container.health.RegisteredHealthCheckActor
import com.typesafe.config.{Config, ConfigFactory}
import configs.syntax._
import hydra.kafka.config.KafkaConfigSupport

import scala.concurrent.duration.FiniteDuration

/**
  * Created by alexsilva on 10/1/16.
  */
trait ClusterHealthCheck extends KafkaConfigSupport with RegisteredHealthCheckActor {

  this: Actor =>

  import scala.collection.JavaConverters._

  protected val channel = applicationConfig.get[String]("notification.channel").valueOrElse("#hydra-ops")

  def interval: FiniteDuration


  protected lazy val producerConfig: Config =
    ConfigFactory.parseMap(Map(
      "metadata.fetch.timeout.ms" -> (interval.toMillis / 2).toString,
      "client.id" -> "hydra.health.check").asJava).withFallback(kafkaProducerFormats("string"))


  protected lazy val consumerConfig: Config =
    ConfigFactory.parseMap(Map(
      "metadata.fetch.timeout.ms" -> (interval.toMillis / 2).toString,
      "client.id" -> "hydra.health.check").asJava).withFallback(kafkaConsumerFormats("string"))
}
