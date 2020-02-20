package hydra.kafka.endpoints

import akka.actor.{Actor, Props}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import hydra.kafka.consumer.KafkaConsumerProxy.{GetPartitionInfo, ListTopics, ListTopicsResponse, PartitionInfoResponse}
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.{Node, PartitionInfo}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class TopicMetadataEndpointSpec extends Matchers
  with WordSpecLike
  with ScalatestRouteTest
  with HydraKafkaJsonSupport
  with BeforeAndAfterAll
  with EmbeddedKafka {

  import spray.json._

  import scala.concurrent.duration._

  implicit val kafkaConfig = EmbeddedKafkaConfig(kafkaPort = 8092, zooKeeperPort = 3181)

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EmbeddedKafka.stop()
  }

  val route = new TopicMetadataEndpoint().route

  val node = new Node(0, "host", 1)

  def partitionInfo(name: String) = new PartitionInfo(name, 0, node, Array(node), Array(node))

  val topics = Map("test1" -> Seq(partitionInfo("test1")))

  private implicit val createTopicFormat = jsonFormat4(CreateTopicReq)

  private implicit val errorFormat = jsonFormat1(CreateTopicResponseError)

  val proxy = system.actorOf(Props(new Actor {
    override def receive: Receive = {
      case ListTopics =>
        sender ! ListTopicsResponse(topics)
      case GetPartitionInfo(topic) =>
        sender ! PartitionInfoResponse(topic, Seq(partitionInfo(topic)))
      case x =>
        throw new RuntimeException(s"did not expect $x")
    }
  }), "kafka_consumer_proxy_test")

  "The topics endpoint" should {

    "returns a list of topics names" in {
      Get("/transports/kafka/topics?fields=name") ~> route ~> check {
        responseAs[Seq[String]] shouldBe Seq("test1")
      }
    }

    "filter out topics by pattern" in {
      Get("/transports/kafka/topics?fields=name&pattern=a.*") ~> route ~> check {
        responseAs[Seq[String]] shouldBe Seq.empty
      }
    }

    "filter topics by pattern" in {
      Get("/transports/kafka/topics?fields=name&pattern=test.*") ~> route ~> check {
        responseAs[Seq[String]] shouldBe Seq("test1")
      }
    }

    "returns a list of topics" in {
      Get("/transports/kafka/topics") ~> route ~> check {
        val r = responseAs[JsObject]
        r.fields("test1") shouldBe Seq(partitionInfo("test1")).toJson
      }
    }

    "returns a topic by name" in {
      Get("/transports/kafka/topics/404") ~> route ~> check {
        response.status.intValue() shouldBe 404
      }
    }

    "returns topic metadata" in {
      Get("/transports/kafka/topics/test1") ~> route ~> check {
        responseAs[JsValue] shouldBe Seq(partitionInfo("test1")).toJson
      }
    }


    "creates a topic" in {
      implicit val timeout = RouteTestTimeout(5.seconds)
      val config = Map(
        "min.insync.replicas" -> "1",
        "cleanup.policy" -> "compact",
        "segment.bytes" -> "1048576"
      )

      val entity = CreateTopicReq("testTopic", 1, 1, config)
      Post("/transports/kafka/topics", entity) ~> route ~> check {
        responseAs[JsValue] shouldBe Seq(partitionInfo("testTopic")).toJson
      }
    }

    "sends back an error response if topic already exists" in {
      implicit val timeout = RouteTestTimeout(5.seconds)
      createCustomTopic("testExisting")(kafkaConfig)
      val config = Map(
        "min.insync.replicas" -> "1",
        "cleanup.policy" -> "compact",
        "segment.bytes" -> "1048576"
      )

      val entity = CreateTopicReq("testExisting", 1, 1, config)
      Post("/transports/kafka/topics", entity) ~> route ~> check {
        response.status.intValue() shouldBe 400
      }
    }

    "sends back an error response if configs are invalid" in {
      implicit val timeout = RouteTestTimeout(5.seconds)

      val config = Map(
        "min.insync.replicas" -> "none",
        "cleanup.policy" -> "under the carpet",
        "segment.bytes" -> "i dont know"
      )

      val entity = CreateTopicReq("test", 1, 1, config)
      Post("/transports/kafka/topics", entity) ~> route ~> check {
        response.status.intValue() shouldBe 400
        responseAs[CreateTopicResponseError]
      }
    }
  }
}
