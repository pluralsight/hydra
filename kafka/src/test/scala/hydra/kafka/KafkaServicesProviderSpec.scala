package hydra.kafka.services

import hydra.common.util.ActorUtils
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.health.KafkaHealthCheckActor
import org.scalatest.{FlatSpecLike, Matchers}

class KafkaServicesProviderSpec extends Matchers with FlatSpecLike {

  "KafkaServiceProviders" should "contain the right services" in {
    KafkaServicesProvider.services.map(_._1) should contain
    allOf(ActorUtils.actorName[KafkaHealthCheckActor], ActorUtils.actorName[KafkaConsumerProxy])
  }

}
