package hydra.jdbc

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import hydra.common.util.TryWith
import hydra.core.transport.HydraRecord
import org.scalatest.{FunSpecLike, Matchers}

class JdbcTransportSpec extends TestKit(ActorSystem("hydra-test")) with Matchers with FunSpecLike
  with ImplicitSender {

  val probe = TestProbe()

  val jdbcTransport = TestActorRef[JdbcTransport](Props[JdbcTransport], "jdbc_transport")

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
  }

  case class TestRecord(destination: String, payload: String, key: Option[String]) extends HydraRecord[String, String]

}


