package hydra.jdbc

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestActors.ForwardActor
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.HYDRA_SCHEMA_PARAM
import hydra.core.protocol._
import hydra.core.transport.AckStrategy.NoAck
import hydra.core.transport.HydraRecord
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class JdbcIngestorSpec extends TestKit(ActorSystem("hydra-test")) with Matchers with FunSpecLike with ImplicitSender
  with BeforeAndAfterAll {

  val ingestor = system.actorOf(Props[JdbcIngestor])

  val probe = TestProbe()

  val jdbcTransport = system.actorOf(Props(new ForwardActor(probe.ref)), "jdbc_transport")

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  
  describe("When using the jdbc ingestor") {
    it("Joins") {
      val request = HydraRequest(123, "someString", Map(JdbcRecordFactory.DB_PROFILE_PARAM -> "testdb"))
      ingestor ! Publish(request)
      expectMsg(Join)
    }

    it("Ignores") {
      val request = HydraRequest(123, "someString")
      ingestor ! Publish(request)
      expectMsg(Ignore)
    }

    it("validates") {
      val r = HydraRequest(123, "someString").withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc",
        JdbcRecordFactory.TABLE_PARAM -> "table")
      ingestor ! Validate(r)
      expectMsgType[InvalidRequest]

      val request = HydraRequest(123,"""{"id":1, "name":"test", "rank" : 1}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc",
          JdbcRecordFactory.TABLE_PARAM -> "table", JdbcRecordFactory.DB_PROFILE_PARAM -> "profile")

      ingestor ! Validate(request)
      expectMsgType[InvalidRequest]

      val r2 = HydraRequest(123,"""{"id":1, "name":"test", "rank" : 1}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc",
          JdbcRecordFactory.TABLE_PARAM -> "table", JdbcRecordFactory.DB_PROFILE_PARAM -> "test-dsprofile")
      ingestor ! Validate(r2)
      expectMsgPF() {
        case ValidRequest(rec) =>
          rec shouldBe a[JdbcRecord]
      }
    }

    it("transports") {
      val supervisor = TestProbe()
      ingestor ! Ingest(TestRecord("test", "test", None), supervisor.ref, NoAck)
      probe.expectMsg(Produce(TestRecord("test", "test", None), supervisor.ref, NoAck))
    }
  }

  case class TestRecord(destination: String, payload: String, key: Option[String]) extends HydraRecord[String, String]

}


