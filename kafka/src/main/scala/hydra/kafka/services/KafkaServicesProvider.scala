package hydra.kafka.services

import akka.actor.Props
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.common.util.ActorUtils
import hydra.core.bootstrap.ServiceProvider
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.health.KafkaHealthCheckActor

import scala.concurrent.duration._

object KafkaServicesProvider extends ServiceProvider with ConfigSupport {

  val healthCheckTopic = applicationConfig
    .getOrElse[String]("kafka.health-check-topic", "_hydra_health_check").value

  override val services = Seq(
    Tuple2(ActorUtils.actorName[KafkaHealthCheckActor],
      Props(classOf[KafkaHealthCheckActor], KafkaConfigSupport.bootstrapServers,
        healthCheckTopic, 20.seconds)),
    Tuple2(ActorUtils.actorName[KafkaConsumerProxy], Props[KafkaConsumerProxy]))
}
