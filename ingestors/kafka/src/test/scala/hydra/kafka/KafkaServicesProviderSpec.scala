package hydra.kafka.services

import hydra.common.util.ActorUtils
import hydra.kafka.consumer.KafkaConsumerProxy
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

class KafkaServicesProviderSpec extends Matchers with AnyFlatSpecLike {

  "KafkaServiceProviders" should "contain the right services" in {
    KafkaServicesProvider.services.map(_._1) should contain(
      ActorUtils.actorName[KafkaConsumerProxy]
    )
  }
}
