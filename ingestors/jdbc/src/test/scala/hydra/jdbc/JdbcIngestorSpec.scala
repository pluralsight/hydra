package hydra.jdbc

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestActors.ForwardActor
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.HYDRA_SCHEMA_PARAM
import hydra.core.protocol._
import hydra.core.transport.AckStrategy.NoAck
import hydra.core.transport.{AckStrategy, HydraRecord}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient

import scala.io.Source
import org.apache.avro.Schema
import hydra.common.config.ConfigSupport
import hydra.avro.registry.ConfluentSchemaRegistry

class JdbcIngestorSpec
    extends TestKit(ActorSystem("jdbc-ingestor-spec"))
    with Matchers
    with FunSpecLike
    with ImplicitSender
    with ConfigSupport
    with BeforeAndAfterAll {

  val ingestor = system.actorOf(Props[JdbcIngestor])

  val probe = TestProbe()

  val jdbcTransport =
    system.actorOf(Props(new ForwardActor(probe.ref)), "jdbc_transport")

  val jdbcTestSchema =
    new Schema.Parser().parse(Source.fromResource("jdbc-test.avsc").mkString)

  val client =
    ConfluentSchemaRegistry.forConfig(applicationConfig).registryClient

  override def beforeAll = {
    client.register("jdbc-test-value", jdbcTestSchema)
  }

  override def afterAll =
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)

  describe("When using the jdbc ingestor") {
    it("Joins") {
      val request = HydraRequest(
        "123",
        "someString",
        None,
        Map(JdbcRecordFactory.DB_PROFILE_PARAM -> "testdb")
      )
      ingestor ! Publish(request)
      expectMsg(Join)
    }

    it("Ignores") {
      val request = HydraRequest("123", "someString")
      ingestor ! Publish(request)
      expectMsg(Ignore)
    }

    it("validates") {
      val r = HydraRequest("123", "someString").withMetadata(
        HYDRA_SCHEMA_PARAM -> "jdbc-test",
        JdbcRecordFactory.TABLE_PARAM -> "table"
      )
      ingestor ! Validate(r)
      expectMsgType[InvalidRequest]

      val request =
        HydraRequest("123", """{"id":1, "name":"test", "rank" : 1}""")
          .withMetadata(
            HYDRA_SCHEMA_PARAM -> "jdbc-test",
            JdbcRecordFactory.TABLE_PARAM -> "table",
            JdbcRecordFactory.DB_PROFILE_PARAM -> "profile"
          )

      ingestor ! Validate(request)
      expectMsgType[InvalidRequest]

      val r2 = HydraRequest("123", """{"id":1, "name":"test", "rank" : 1}""")
        .withMetadata(
          HYDRA_SCHEMA_PARAM -> "jdbc-test",
          JdbcRecordFactory.TABLE_PARAM -> "table",
          JdbcRecordFactory.DB_PROFILE_PARAM -> "test-dsprofile"
        )
      ingestor ! Validate(r2)
      expectMsgPF() {
        case ValidRequest(rec) =>
          rec shouldBe a[JdbcRecord]
      }
    }

    it("transports") {
      ingestor ! Ingest(
        TestRecord("test", "test", "", AckStrategy.NoAck),
        NoAck
      )
      probe.expectMsg(
        Produce(TestRecord("test", "test", "", AckStrategy.NoAck), self, NoAck)
      )
    }
  }

  case class TestRecord(
      destination: String,
      payload: String,
      key: String,
      ackStrategy: AckStrategy
  ) extends HydraRecord[String, String]

}
