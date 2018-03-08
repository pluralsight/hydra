package hydra.kafka

import hydra.kafka.util.KafkaUtils
import org.scalatest._

/**
  * Created by alexsilva on 5/17/17.
  */
class KafkaUtilsErrorSpec extends Matchers
  with FlatSpecLike
  with BeforeAndAfterAll {

  "Kafka Utils (Error)" should "return false when trying to find a topic in error" in {
    assert(!KafkaUtils.topicExists("test_123123").isSuccess)
  }
}
