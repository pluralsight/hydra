package hydra.kafka.services

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.util.KafkaUtils
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

class CompactedTopicStreamActorSpec extends TestKit(ActorSystem("compacted-stream-actor-spec"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockFactory
  with ScalaFutures
  with EmbeddedKafka
  with HydraKafkaJsonSupport
  with Eventually {

  implicit val ec = system.dispatcher

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false"))

  val bootstrapConfig = ConfigFactory.load().getConfig("hydra_kafka.bootstrap-config")

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5000 millis),
    interval = scaled(1000 millis))

  val topic = "test.topic"
  val compactedTopic = "_compacted.test.topic"
  val bootstrapServers = KafkaUtils.BootstrapServers



  override def beforeAll: Unit = {
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic(topic)

    publishStringMessageToKafka(topic, "message")
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  "The CompactedTopicStreamActor" should "stream from a non compacted topic to a compacted topic in" in {

    EmbeddedKafka.createCustomTopic(compactedTopic)

    val probe = TestProbe()
    val compactedStreamActor = system.actorOf(
      CompactedTopicStreamActor.props(topic, compactedTopic, bootstrapServers, bootstrapConfig),
      name = "compacted_topic_stream_actor")

    consumeFirstStringMessageFrom(compactedTopic) shouldEqual "message"


  }

  "The CompactedTopicStreamActor" should "create a compacted topic and stream if it doesn't exist already" in {

    val probe = TestProbe()
    val compactedStreamActor = system.actorOf(
      CompactedTopicStreamActor.props(topic, compactedTopic, bootstrapServers, bootstrapConfig),
      name = "ctsa")

    eventually {
      consumeFirstStringMessageFrom(compactedTopic) shouldEqual "message"
    }
  }



}


