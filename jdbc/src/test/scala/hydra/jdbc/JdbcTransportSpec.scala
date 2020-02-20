package hydra.jdbc

import java.sql.SQLException

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import cats.effect.{IO, Timer}
import cats.implicits._
import hydra.common.logging.LoggingAdapter
import hydra.common.util.TryWith
import hydra.core.transport.Transport.Deliver
import hydra.core.transport.{AckStrategy, HydraRecord, RecordMetadata, TransportCallback}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import retry.RetryDetails._
import retry.RetryPolicies._
import retry.{RetryPolicy, _}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.io.Source

class JdbcTransportSpec extends TestKit(ActorSystem("jdbc-transport-spec")) with Matchers with FunSpecLike
  with ImplicitSender with BeforeAndAfterAll with LoggingAdapter {

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
      TryWith(profiles("test-dsprofile").ds.getConnection()) { c =>
        c.getMetaData.getURL shouldBe "jdbc:h2:mem:test_db"
      }
    }

    it("reports error if profile can't be found") {
      val record = JdbcRecord("dest", Seq.empty, gr, "dbProfile", AckStrategy.NoAck)
      jdbcTransport ! Deliver(record, 1, ack)
      probe.expectMsgType[NoSuchElementException]
    }

    it("transports") {
      val record = JdbcRecord("test_transport", Seq.empty, gr, "test-dsprofile", AckStrategy.NoAck)
      jdbcTransport ! Deliver(record, 1, ack)
      probe.expectMsg("DONE")
      //check the db too
      val profile = jdbcTransport.underlyingActor.dbProfiles("test-dsprofile")

      TryWith(profile.ds.getConnection()) { c =>
        val stmt = c.createStatement()
        val rs = stmt.executeQuery("select \"id\",\"name\" from test_transport")
        rs.next() shouldBe true
      }
    }

    val retryPolicy: RetryPolicy[IO] = limitRetries[IO](4) |+| exponentialBackoff[IO](500 millis)
    implicit val sleep: Timer[IO] = IO.timer(ExecutionContext.global)

    def logError(err: Throwable, details: RetryDetails): IO[Unit] = details match {
      case WillDelayAndRetry(_: FiniteDuration, retriesSoFar: Int, _: FiniteDuration) =>
        IO(log.debug(s"DBProfile not yet closed. So far we have retried $retriesSoFar times."))
      case GivingUp(totalRetries: Int, duration: FiniteDuration) =>
        IO(log.debug(s"DBProfile could not be closed. Giving up after $totalRetries retries and ${duration.toSeconds} seconds."))
    }

    it("errors if underlying datasource is closed") {
      val jt = TestActorRef[JdbcTransport](Props[JdbcTransport])
      val profile = jt.underlyingActor.dbProfiles("test-dsprofile")
      profile.close()

      def profileIsClosed = if (profile.isClosed) IO.unit else IO.raiseError(new Exception)
      val flakyRequestWithRetry: IO[Unit] = retryingOnAllErrors[Unit](policy = retryPolicy, onError = logError)(profileIsClosed)

      (for {
        _ <- flakyRequestWithRetry
      } yield {
        val record = JdbcRecord("test_transport", Seq.empty, gr, "test-dsprofile", AckStrategy.NoAck)
        jt ! Deliver(record, 1, ack)
        probe.expectMsgType[SQLException]
        system.stop(jt)
      }).unsafeRunSync()
    }
  }

  case class TestRecord(destination: String, payload: String, key: String, ackStrategy: AckStrategy) extends HydraRecord[String, String]

}


