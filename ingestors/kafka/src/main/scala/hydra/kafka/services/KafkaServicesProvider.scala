package hydra.kafka.services

import akka.actor.Props
import hydra.common.config.ConfigSupport
import ConfigSupport._
import hydra.common.util.ActorUtils
import hydra.core.bootstrap.ServiceProvider
import hydra.kafka.consumer.KafkaConsumerProxy

import scala.concurrent.duration._

object KafkaServicesProvider extends ServiceProvider with ConfigSupport {

  val healthCheckTopic = applicationConfig
    .getStringOpt("kafka.health-check-topic").getOrElse("_hydra_health_check")

  val interval = applicationConfig
    .getDurationOpt("kafka.health_check.interval").getOrElse(20.seconds)

  override val services = Seq(
    Tuple2(ActorUtils.actorName[KafkaConsumerProxy], Props[KafkaConsumerProxy])
  )
}
