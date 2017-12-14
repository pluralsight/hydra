package hydra.kafka

import akka.actor.ActorSystem
import akka.testkit.TestKit
import hydra.kafka.util.KafkaUtils
import org.scalatest._


/**
  * Created by alexsilva on 5/17/17.
  */
class KafkaUtilsErrorSpec extends TestKit(ActorSystem("hydra")) with Matchers with FlatSpecLike with BeforeAndAfterAll {
  "Kafka Utils" should "return false when trying to find a topic in error" in {
    assert(!KafkaUtils.topicExists("test_123123").isSuccess)
  }
}

