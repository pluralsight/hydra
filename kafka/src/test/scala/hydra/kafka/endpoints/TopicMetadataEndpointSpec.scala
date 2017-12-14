package hydra.kafka.endpoints

import akka.actor.{Actor, Props}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestKit
import hydra.kafka.consumer.KafkaConsumerProxy.{ListTopics, ListTopicsResponse}
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import org.apache.kafka.common.{Node, PartitionInfo}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class TopicMetadataEndpointSpec extends Matchers with WordSpecLike with ScalatestRouteTest
  with HydraKafkaJsonSupport with BeforeAndAfterAll {

  import spray.json._
  import scala.concurrent.duration._

  override def cleanUp(): Unit = TestKit.shutdownActorSystem(system, duration = 30.seconds)

  val route = new TopicMetadataEndpoint().route

  val node = new Node(0, "host", 1)
  val partitionInfo = new PartitionInfo("test1", 0, node, Array(node), Array(node))
  val topics = Map("test1" -> Seq(partitionInfo))

  val proxy = system.actorOf(Props(new Actor {
    override def receive: Receive = {
      case ListTopics => sender ! ListTopicsResponse(topics)
    }
  }), "kafka_consumer_proxy_test")

  "The topics endpoint" should {

    "returns a list of topics names" in {
      Get("/transports/kafka/topics?names") ~> route ~> check {
        responseAs[Seq[String]] shouldBe Seq("test1")
      }
    }

    "returns a list of topics" in {
      Get("/transports/kafka/topics") ~> route ~> check {
        val r = responseAs[JsObject]
        r.fields("test1") shouldBe Seq(partitionInfo).toJson
      }
    }

    "returns a topic by name" in {
      Get("/transports/kafka/topics/404") ~> route ~> check {
        response.status.intValue() shouldBe 404
      }
    }

    "returns topic metadata" in {
      Get("/transports/kafka/topics/test1") ~> route ~> check {
        responseAs[JsValue] shouldBe Seq(partitionInfo).toJson
      }
    }
  }
}
