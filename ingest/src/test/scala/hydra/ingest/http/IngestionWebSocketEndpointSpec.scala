package hydra.ingest.http

import akka.actor.Actor
import akka.http.scaladsl.model.ws.{BinaryMessage, TextMessage}
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import akka.pattern.pipe
import akka.stream.scaladsl.Source
import akka.testkit.{TestActorRef, TestKit}
import akka.util.ByteString
import hydra.core.protocol._
import hydra.ingest.IngestorInfo
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName, LookupResult}
import hydra.ingest.test.TestRecordFactory
import org.joda.time.DateTime
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

/**
  * Created by alexsilva on 5/12/17.
  */
class IngestionWebSocketEndpointSpec
    extends Matchers
    with AnyWordSpecLike
    with ScalatestRouteTest {

  val endpt = new IngestionWebSocketEndpoint()

  override def afterAll: Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10 seconds
    )
  }

  val ingestor = TestActorRef(
    new Actor {

      override def receive: Receive = {
        case Publish(_) => sender ! Join
        case Validate(r) =>
          TestRecordFactory.build(r).map(ValidRequest(_)) pipeTo sender
        case Ingest(req, _) if req.payload == "error" =>
          sender ! IngestorError(new IllegalArgumentException)
        case Ingest(_, _) => sender ! IngestorCompleted
      }
    },
    "test_ingestor"
  )

  val ingestorInfo =
    IngestorInfo("test_ingestor", "test", ingestor.path, DateTime.now)

  val registry = TestActorRef(
    new Actor {

      override def receive: Receive = {
        case FindByName("tester") => sender ! LookupResult(Seq(ingestorInfo))
        case FindAll              => sender ! LookupResult(Seq(ingestorInfo))
      }
    },
    "ingestor_registry"
  )

  "the IngestionWebSocketEndpoint" should {

    "returned a 409 if not enabled" in {
      val endpt = new IngestionWebSocketEndpoint() {
        override val enabled = false
      }
      val wsClient = WSProbe()
      WS("/ws-ingest", wsClient.flow) ~> endpt.route ~> check {
        response.status.intValue() shouldBe 409
      }

    }

    "handle websocket requests" in {
      val wsClient = WSProbe()

      WS("/ws-ingest", wsClient.flow) ~> endpt.route ~> check {
        // check response for WS Upgrade headers
        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage("-c SET hydra-kafka-topic = test.Topic")
        wsClient.expectMessage(
          """{"message":"OK[HYDRA-KAFKA-TOPIC=test.Topic]","status":200}"""
        )
        wsClient.sendMessage("-c SET hydra-ack = explicit")
        wsClient.expectMessage(
          """{"message":"BAD REQUEST[hydra-ack=explicit] is not a valid ack strategy.","status":400}"""
        )

        wsClient.sendMessage("-c SET hydra-ack = replicated")
        wsClient.expectMessage(
          """{"message":"OK[hydra-ack=replicated]","status":200}"""
        )

        wsClient.sendMessage("-c WHAT")
        wsClient.expectMessage(
          """{"message":"BAD_REQUEST:Not a valid message. Use 'HELP' for help.","status":400}"""
        )

        wsClient.sendMessage("-c SET")
        wsClient.expectMessage(
          """{"message":"HYDRA-KAFKA-TOPIC -> test.Topic;hydra-ack -> Replicated","status":200}"""
        )

        wsClient.sendMessage("-c HELP")
        wsClient.expectMessage(
          """{"message":"Set metadata: --set (name)=(value)","status":200}"""
        )

        wsClient.sendMessage("""{"name":"test","value":"test"}""")
        wsClient.expectMessage(
          """{"correlationId":"0","ingestors":{"test_ingestor":{"code":200,"message":"OK"}}}"""
        )

        wsClient.sendMessage("""-i 122 {"name":"test","value":"test"}""")
        wsClient.expectMessage(
          """{"correlationId":"122","ingestors":{"test_ingestor":{"code":200,"message":"OK"}}}"""
        )

        wsClient.sendMessage("error")
        wsClient.expectMessage(
          """{"correlationId":"0","ingestors":{"test_ingestor":{"code":503,"message":"Unknown error."}}}"""
        )

        wsClient.sendCompletion()
        wsClient.expectCompletion()
      }

    }

    "sets metadata" in {
      val wsClient = WSProbe()

      WS("/ws-ingest", wsClient.flow) ~> endpt.route ~> check {
        // check response for WS Upgrade headers
        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage("-c SET hydra-delivery-strategy = at-most-once")
        wsClient.expectMessage(
          """{"message":"OK[HYDRA-DELIVERY-STRATEGY=at-most-once]","status":200}"""
        )
        wsClient.sendMessage("-c SET hydra-client-id = test-client")
        wsClient.expectMessage(
          """{"message":"OK[HYDRA-CLIENT-ID=test-client]","status":200}"""
        )

        wsClient.sendMessage("""-i 122 {"name":"test","value":"test"}""")
        wsClient.expectMessage(
          """{"correlationId":"122","ingestors":{"test_ingestor":{"code":200,"message":"OK"}}}"""
        )

        wsClient.sendCompletion()
        wsClient.expectCompletion()
      }

    }

    "handle ws message with more than one frame" in {
      val wsClient = WSProbe()

      WS("/ws-ingest", wsClient.flow) ~> endpt.route ~> check {
        // check response for WS Upgrade headers
        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage(
          TextMessage.Streamed(
            Source("-c SET hydra-delivery-" :: "strategy = at-most-once" :: Nil)
          )
        )
        wsClient.expectMessage(
          """{"message":"OK[HYDRA-DELIVERY-STRATEGY=at-most-once]","status":200}"""
        )

        wsClient.sendCompletion()
        wsClient.expectCompletion()
      }
    }

    "reject ws message with too many frames" in {
      val wsClient = WSProbe()

      WS("/ws-ingest", wsClient.flow) ~> endpt.route ~> check {
        // check response for WS Upgrade headers
        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage(
          TextMessage.Streamed(
            Source(1 to IngestionWebSocketEndpoint.maxNumberOfWSFrames + 1)
              .map(_.toString)
          )
        )
        wsClient.expectMessage(
          """{"message":"Frame limit reached after frame number 50.","status":400}"""
        )

        wsClient.sendMessage(
          TextMessage.Streamed(
            Source(1 to 10)
              .map(_.toString)
              .throttle(1, IngestionWebSocketEndpoint.streamedWSMessageTimeout)
          )
        )
        wsClient.expectMessage(
          """{"message":"Timeout on frame buffer reached.","status":400}"""
        )

        wsClient.sendMessage(BinaryMessage.apply(ByteString("")))
        wsClient.expectMessage(
          """{"message":"Binary messages are not supported.","status":400}"""
        )

        wsClient.sendMessage(
          TextMessage.Streamed(
            Source("-c SET hydra-delivery-" :: "strategy = at-most-once" :: Nil)
          )
        )
        wsClient.expectMessage(
          """{"message":"OK[HYDRA-DELIVERY-STRATEGY=at-most-once]","status":200}"""
        )

        wsClient.sendCompletion()
        wsClient.expectCompletion()
      }
    }

  }
}
