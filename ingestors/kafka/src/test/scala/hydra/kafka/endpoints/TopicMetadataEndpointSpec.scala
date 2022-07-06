package hydra.kafka.endpoints

import akka.actor.{Actor, ActorRef, ActorSelection, Props}
import akka.http.javadsl.server.MalformedRequestContentRejection
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import cats.Applicative
import cats.effect.{Concurrent, ContextShift, IO, Sync, Timer}
import hydra.avro.registry.SchemaRegistry
import hydra.common.NotificationsTestSuite
import hydra.common.alerting.sender.InternalNotificationSender
import hydra.common.config.ConfigSupport
import hydra.common.util.ActorUtils
import hydra.core.http.CorsSupport
import hydra.kafka.algebras.{HydraTag, KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra, TagsAlgebra}
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.consumer.KafkaConsumerProxy.{GetPartitionInfo, ListTopics, ListTopicsResponse, PartitionInfoResponse}
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.model.RequiredField
import hydra.kafka.model.TopicMetadataV2Request.Subject
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.{Node, PartitionInfo}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext
import hydra.kafka.programs.{CreateTopicProgram, KeyAndValueSchemaV2Validator}
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import retry.{RetryPolicies, RetryPolicy}

import java.time.Instant


class TopicMetadataEndpointSpec
    extends Matchers
    with AnyWordSpecLike
    with ScalatestRouteTest
    with HydraKafkaJsonSupport
    with BeforeAndAfterAll
    with EmbeddedKafka
    with ConfigSupport
    with NotificationsTestSuite {

  import spray.json._

  import scala.concurrent.duration._

  import ConfigSupport._

  implicit private def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] =
    Slf4jLogger.getLogger[F]

  implicit val kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = 8012, zooKeeperPort = 3111)

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val concurrent: Concurrent[IO] = IO.ioConcurrentEffect
  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  private implicit val corsSupport: CorsSupport = new CorsSupport("http://*")
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

  private def getTestCreateTopicProgram(
                                         s: SchemaRegistry[IO],
                                         ka: KafkaAdminAlgebra[IO],
                                         kc: KafkaClientAlgebra[IO],
                                         m: MetadataAlgebra[IO]
                                       ): CreateTopicProgram[IO] = {
    val retryPolicy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
      CreateTopicProgram.make[IO](
        s,
        ka,
        kc,
        retryPolicy,
        Subject.createValidated("dvs.hello-world").get,
        m
      )
  }

  private def getSchema[F[_]: Applicative](name: String): F[Schema] =
    Applicative[F].pure {
      SchemaBuilder
        .record(name)
        .fields()
        .name("isTrue")
        .doc("text")
        .`type`()
        .stringType()
        .noDefault()
        .name(RequiredField.CREATED_AT)
        .doc("text")
        .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .withDefault(Instant.now().toEpochMilli)
        .name(RequiredField.UPDATED_AT)
        .doc("text")
        .`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .withDefault(Instant.now().toEpochMilli)
        .endRecord()
    }

  val subjectKey = "dvs.test.subject-key"
  val subjectValue = "dvs.test.subject-value"

  val route: Route = {
    implicit val notificationSenderMock: InternalNotificationSender[IO] = getInternalNotificationSenderMock[IO]
    for {
    kafkaClient <- KafkaClientAlgebra.test[IO]
    schemaRegistry <- SchemaRegistry.test[IO]
    ka <- KafkaAdminAlgebra.test[IO]()
    schema <- getSchema("dvs.test.subject")
    _ <- schemaRegistry.registerSchema(subjectKey, schema)
    _ <- schemaRegistry.registerSchema(subjectValue, schema)
    _ <- ka.createTopic("dvs.test.subject", TopicDetails(1,1,1))
    metadataAlgebra <- MetadataAlgebra.make[IO](Subject.createValidated("_topicName.Bill").get, "I'm_A_Jerk", kafkaClient, schemaRegistry, consumeMetadataEnabled = false)
    tagsAlgebra <- TagsAlgebra.make[IO]("_hydra.tags-topic", "_hydra.tags-consumer",kafkaClient)
    _ <- tagsAlgebra.createOrUpdateTag(HydraTag("Source: DVS", "A valid source"))
    createTopicProgram = getTestCreateTopicProgram(schemaRegistry, ka, kafkaClient, metadataAlgebra)
  } yield new TopicMetadataEndpoint(consumerProxy, metadataAlgebra, schemaRegistry, createTopicProgram, 1, tagsAlgebra).route}.unsafeRunSync()

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

  "The /v2/metadata endpoint" should {

    val validRequest = """{
                         |    "streamType": "Event",
                         |    "deprecated": true,
                         |    "dataClassification": "InternalUseOnly",
                         |    "contact": {
                         |        "email": "bob@myemail.com"
                         |    },
                         |    "createdDate": "2020-02-02T12:34:56Z",
                         |    "notes": "here are some notes",
                         |    "parentSubjects": [],
                         |    "teamName": "dvs-teamName",
                         |    "tags": ["Source: DVS"],
                         |    "notificationUrl": "testnotification.url"
                         |}""".stripMargin

    val invalidRequest =
      """{
        |    "streamType": "History",
        |    "deprecated": true,
        |    "dataClassification": "InternalUseOnly",
        |    "contact": {
        |        "email": "bob@myemail.com"
        |    },
        |    "createdDate": "2020-02-02T12:34:56Z",
        |    "notes": "here are some notes",
        |    "parentSubjects": [],
        |    "teamName": "dvs-teamName"
        |}""".stripMargin

    val invalidRequestWithEmptyTagList =
    """{
       |    "streamType": "Event",
       |    "deprecated": true,
       |    "dataClassification": "InternalUseOnly",
       |    "contact": {
       |        "email": "bob@myemail.com"
       |    },
       |    "createdDate": "2020-02-02T12:34:56Z",
       |    "notes": "here are some notes",
       |    "parentSubjects": [],
       |    "teamName": "dvs-teamName",
       |    "tags": []
       |}""".stripMargin

    "return 200 with proper metadata" in {
      implicit val timeout = RouteTestTimeout(5.seconds)
      Put("/v2/metadata/dvs.test.subject", HttpEntity(ContentTypes.`application/json`, validRequest)) ~> route ~> check {
        response.status shouldBe StatusCodes.OK
      }
    }

    "return 400 with missing schemas" in {
      Put("/v2/metadata/dvs.subject.noschema", HttpEntity(ContentTypes.`application/json`, validRequest)) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "return 400 with empty tag list" in {
      Put("/v2/metadata/dvs.test.subject", HttpEntity(ContentTypes.`application/json`, invalidRequestWithEmptyTagList)) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "reject invalid metadata" in {
      Put("/v2/metadata/dvs.test.subject", HttpEntity(ContentTypes.`application/json`, invalidRequest)) ~> route ~> check {
        rejection shouldBe a[MalformedRequestContentRejection]
      }
    }
  }
}
