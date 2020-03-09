package hydra.kafka.services

import hydra.common.util.ActorUtils
import hydra.kafka.consumer.KafkaConsumerProxy
import org.scalatest.{FlatSpecLike, Matchers}

class KafkaServicesProviderSpec extends Matchers with FlatSpecLike {

  "KafkaServiceProviders" should "contain the right services" in {
    KafkaServicesProvider.services.map(_._1) should contain(
      ActorUtils.actorName[KafkaConsumerProxy]
    )
  }
}
