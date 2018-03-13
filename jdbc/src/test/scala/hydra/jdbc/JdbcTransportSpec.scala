package hydra.jdbc

import java.sql.SQLException

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import hydra.common.util.TryWith
import hydra.core.transport.Transport.Deliver
import hydra.core.transport.{HydraRecord, RecordMetadata, TransportCallback}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.io.Source

class JdbcTransportSpec extends TestKit(ActorSystem("jdbc-transpport-spec")) with Matchers with FunSpecLike
  with ImplicitSender with BeforeAndAfterAll {

  val probe = TestProbe()

  val schema = new Schema.Parser().parse(Source.fromResource("jdbc-test.avsc").mkString)

  val jdbcTransport = TestActorRef[JdbcTransport](Props[JdbcTransport], "jdbc_transport")

  val ack: TransportCallback = (d: Long, md: Option[RecordMetadata], err: Option[Throwable]) =>
    probe.ref ! md.map(_ => "DONE").getOrElse(err.get)

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true)

  val gr = new GenericData.Record(schema)
  gr.put("id", 1)
  gr.put("name", "alex")
  gr.put("rank", 2)

  describe("The Jdbc transport") {
    it("loads db profiles on preStart") {
      val profiles = jdbcTransport.underlyingActor.dbProfiles
      profiles.keySet should contain("test-dsprofile")
      profiles("test-dsprofile").name shouldBe "test-dsprofile"
      TryWith(profiles("test-dsprofile").ds.getConnection) { c =>
        c.getMetaData.getURL shouldBe "jdbc:h2:mem:test_db"
      }
    }

    it("looks up the db profile url") {
      val profiles = jdbcTransport.underlyingActor.dbProfiles
      JdbcTransport.getUrl(profiles("test-dsprofile")) shouldBe "jdbc:h2:mem:test_db"
      JdbcTransport.getUrl(profiles("test-jdbcprofile")) shouldBe "jdbc:h2:mem:test_jdb;DB_CLOSE_DELAY=-1"
    }

    it("reports error if profile can't be found") {
      val record = JdbcRecord("dest", Some(Seq.empty), gr, "dbProfile")
      jdbcTransport ! Deliver(record, 1, ack)
      probe.expectMsgType[NoSuchElementException]
    }

    it("transports") {
      val record = JdbcRecord("test_transport", Some(Seq.empty), gr, "test-dsprofile")
      jdbcTransport ! Deliver(record, 1, ack)
      probe.expectMsg("DONE")
      //check the db too
      TryWith(jdbcTransport.underlyingActor.dbProfiles("test-dsprofile").ds.getConnection()) { c =>
        val stmt = c.createStatement()
        val rs = stmt.executeQuery("select \"id\",\"name\" from test_transport")
        rs.next() shouldBe true
      }
    }

    it("errors if underlying datasource is closed") {
      val jt = TestActorRef[JdbcTransport](Props[JdbcTransport])
      jt.underlyingActor.dbProfiles("test-dsprofile").ds.close()
      val record = JdbcRecord("test_transport", Some(Seq.empty), gr, "test-dsprofile")
      jt ! Deliver(record, 1, ack)
      probe.expectMsgType[SQLException]
      system.stop(jt)
    }
  }

  case class TestRecord(destination: String, payload: String, key: Option[String]) extends HydraRecord[String, String]

}


