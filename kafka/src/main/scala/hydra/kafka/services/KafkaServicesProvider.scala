package hydra.kafka.services

import akka.actor.Props
import hydra.common.util.ActorUtils
import hydra.core.app.ServiceProvider
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.health.KafkaHealthCheckActor
import scala.concurrent.duration._
import configs.syntax._

object KafkaServicesProvider extends ServiceProvider with KafkaConfigSupport {

  val healthCheckTopic = applicationConfig.getOrElse[String]("kafka.health-check-topic", "__hydra_health_check").value

  override val services = Seq(
    Tuple2(ActorUtils.actorName[KafkaHealthCheckActor], Props(classOf[KafkaHealthCheckActor], bootstrapServers,
      healthCheckTopic, 20.seconds)),
    Tuple2(ActorUtils.actorName[KafkaConsumerProxy], Props[KafkaConsumerProxy]))
}
