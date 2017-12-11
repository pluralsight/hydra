package hydra.kafka.endpoints

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestKit
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class StreamingTopicEndpointSpec extends Matchers with WordSpecLike with ScalatestRouteTest
  with HydraKafkaJsonSupport with BeforeAndAfterAll {

  import scala.concurrent.duration._

  override def cleanUp(): Unit = TestKit.shutdownActorSystem(system, duration = 30.seconds)

  val route = new StreamingTopicsEndpoint().route

  //  val proxy = system.actorOf(Props(new Actor {
  //    override def receive: Receive = {
  //      case ListTopics => sender ! ListTopicsResponse(topics)
  //    }
  //  }), "kafka_consumer_proxy_test")

  "The topics streaming endpoint" should {

    implicit val config = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181,
      customBrokerProperties = Map("auto.create.topics.enable" -> "false"))

    "return 503 if zookeeper is down" in {
      Get("/transports/kafka/streaming/unknown") ~> route ~> check {
        response.status.intValue() shouldBe 503
      }
    }
  }
}
