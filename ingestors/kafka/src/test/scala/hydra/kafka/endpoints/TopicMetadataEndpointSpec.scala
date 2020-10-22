package hydra.kafka.endpoints

import akka.actor.{Actor, ActorRef, ActorSelection, Props}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import cats.effect.{Concurrent, ContextShift, IO, Sync}
import hydra.avro.registry.SchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.common.util.ActorUtils
import hydra.kafka.algebras.{KafkaClientAlgebra, MetadataAlgebra}
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.consumer.KafkaConsumerProxy.{GetPartitionInfo, ListTopics, ListTopicsResponse, PartitionInfoResponse}
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.model.TopicMetadataV2Request.Subject
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.{Node, PartitionInfo}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext


class TopicMetadataEndpointSpec
    extends Matchers
    with AnyWordSpecLike
    with ScalatestRouteTest
    with HydraKafkaJsonSupport
    with BeforeAndAfterAll
    with EmbeddedKafka
    with ConfigSupport {

  import spray.json._

  import scala.concurrent.duration._

  import ConfigSupport._

  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] =
    Slf4jLogger.getLogger[F]

  implicit val kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = 8012, zooKeeperPort = 3111)

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val concurrent: Concurrent[IO] = IO.ioConcurrentEffect

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EmbeddedKafka.stop()
  }

  val consumerPath: String = applicationConfig
    .getStringOpt("actors.kafka.consumer_proxy.path")
    .getOrElse(
      s"/user/service/${ActorUtils.actorName(classOf[KafkaConsumerProxy])}"
    )

  val consumerProxy: ActorSelection = system.actorSelection(consumerPath)

  val route: Route = (for {
    kafkaClient <- KafkaClientAlgebra.test[IO]
    schemaRegistry <- SchemaRegistry.test[IO]
    metadataAlgebra <- MetadataAlgebra.make[IO]("topicName-Bill", "I'm_A_Jerk", kafkaClient, schemaRegistry, consumeMetadataEnabled = false)
  } yield new TopicMetadataEndpoint(consumerProxy, metadataAlgebra).route).unsafeRunSync()

  val node = new Node(0, "host", 1)

  def partitionInfo(name: String) =
    new PartitionInfo(name, 0, node, Array(node), Array(node))

  val topics = Map("test1" -> Seq(partitionInfo("test1")))

  private implicit val createTopicFormat: RootJsonFormat[CreateTopicReq] = jsonFormat4(CreateTopicReq)

  private implicit val errorFormat: RootJsonFormat[CreateTopicResponseError] = jsonFormat1(CreateTopicResponseError)

  val proxy: ActorRef = system.actorOf(
    Props(new Actor {

      override def receive: Receive = {
        case ListTopics =>
          sender ! ListTopicsResponse(topics)
        case GetPartitionInfo(topic) =>
          sender ! PartitionInfoResponse(topic, Seq(partitionInfo(topic)))
        case x =>
          throw new RuntimeException(s"did not expect $x")
      }
    }),
    "kafka_consumer_proxy_test"
  )

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

  "The /v2/topics GET endpoint" should {
    "retrieve empty array of metadata" in {
      Get("/v2/topics/") ~> route ~> check {
        response.status shouldBe StatusCodes.OK
      }
    }

    "recieve 404 with Subject not found body" in {
      Get("/v2/topics/skills.subject/") ~> route ~> check {
        response.status shouldBe StatusCodes.NotFound
        responseAs[String] shouldBe "Subject skills.subject could not be found."
      }
    }

    "receive 400 with Subject not properly formatted" in {
      Get("/v2/topics/invalid!topicasf/") ~> route ~> check {
        response.status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe Subject.invalidFormat
      }
    }
  }
}
