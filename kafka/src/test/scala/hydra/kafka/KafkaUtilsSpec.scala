package hydra.kafka

import hydra.kafka.util.KafkaUtils
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{BeforeAndAfterAll, WordSpec}

/**
  * Created by alexsilva on 5/17/17.
  */
class KafkaUtilsSpec extends WordSpec with EmbeddedKafka with BeforeAndAfterAll {

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181)

  override def beforeAll() = {
    EmbeddedKafka.start()
  }

  "Kafka Utils" should {
    "return false for a topic that doesn't exist" in {
      assert(!KafkaUtils.topicExists("test"))
    }

    "return true for a topic that exists" in {
      EmbeddedKafka.createCustomTopic("test-topic")
      assert(KafkaUtils.topicExists("test-topic"))
    }
  }

  override def afterAll() = {
    EmbeddedKafka.stop()
  }
}
