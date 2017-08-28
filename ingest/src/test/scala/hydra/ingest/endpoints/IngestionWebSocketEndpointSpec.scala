package hydra.ingest.endpoints

import akka.actor.Actor
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import akka.testkit.TestActorRef
import hydra.core.protocol._
import hydra.ingest.ingestors.IngestorInfo
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName, LookupResult}
import org.joda.time.DateTime
import org.scalatest.{Matchers, WordSpecLike}

/**
  * Created by alexsilva on 5/12/17.
  */
class IngestionWebSocketEndpointSpec extends Matchers with WordSpecLike with ScalatestRouteTest {

  val ingestor = TestActorRef(new Actor {
    override def receive = {
      case Publish(_) => sender ! Join
      case Validate(_) => sender ! ValidRequest
      case Ingest(_) => sender ! IngestorCompleted
    }
  }, "test_ingestor")


  val ingestorInfo = IngestorInfo("test_ingestor", "test", ingestor.path, DateTime.now)

  val registry = TestActorRef(new Actor {
    override def receive = {
      case FindByName("tester") => sender ! LookupResult(Seq(ingestorInfo))
      case FindAll => sender ! LookupResult(Seq(ingestorInfo))
    }
  }, "ingestor_registry")

  val endpt = new IngestionWebSocketEndpoint()
  "the IngestionWebSocketEndpoint" should {

    "handle websocket requests" in {

      val wsClient = WSProbe()

      WS("/ws-ingest", wsClient.flow) ~> endpt.handleWebSocketMessages(endpt.createSocket("", Seq.empty)) ~>
        check {
          // check response for WS Upgrade headers
          isWebSocketUpgrade shouldEqual true

          //set some stuff
          wsClient.sendMessage("-c SET hydra-kafka-topic = test.Topic")
          wsClient.expectMessage("""{"status":200,"message":"OK[HYDRA-KAFKA-TOPIC=test.Topic]"}""")
          wsClient.sendMessage("-c SET hydra-ack = explicit")
          wsClient.expectMessage("""{"status":200,"message":"OK[HYDRA-ACK=explicit]"}""")

          wsClient.sendMessage("-c SET")
          wsClient.expectMessage("""{"status":200,"message":"HYDRA-KAFKA-TOPIC -> test.Topic;HYDRA-ACK -> explicit"}""")

          wsClient.sendMessage("-c HELP")
          wsClient.expectMessage("""{"status":200,"message":"Set metadata: --set (name)=(value)"}""")


          wsClient.sendMessage("""{"name":"test","value":"test"}""")
          wsClient.expectMessage("""{"correlationId":0,"ingestors":{"test_ingestor":{"code":200,"message":"OK"}}}""")

          // manually run a WS conversation
          wsClient.sendMessage("""-i 122 {"name":"test","value":"test"}""")
          wsClient.expectMessage("""{"correlationId":122,"ingestors":{"test_ingestor":{"code":200,"message":"OK"}}}""")


          wsClient.sendCompletion()

          wsClient.expectCompletion()

        }
    }
  }
}
