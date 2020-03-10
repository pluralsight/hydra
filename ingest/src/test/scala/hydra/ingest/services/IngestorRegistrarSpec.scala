package hydra.ingest.services

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import hydra.common.util.ActorUtils
import hydra.ingest.services.IngestorRegistrar.UnregisterAll
import hydra.ingest.services.IngestorRegistry.{
  FindAll,
  FindByName,
  LookupResult
}
import hydra.ingest.test.TestIngestor
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/9/17.
  */
class IngestorRegistrarSpec
    extends TestKit(ActorSystem("IngestorRegistrarSpec"))
    with Matchers
    with AnyFunSpecLike
    with ImplicitSender
    with ScalaFutures
    with BeforeAndAfterAll
    with Eventually {

  override def afterAll =
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(1, Seconds))

  val registry = system.actorOf(Props[IngestorRegistry], "ingestor_registry")

  val act = system.actorOf(Props[IngestorRegistrar])

  implicit val timeout = Timeout(3, TimeUnit.SECONDS)

  describe("The ingestor registrar actor") {
    it("registers from classpath on bootstrap") {
      eventually {
        whenReady(
          (registry ? FindByName(ActorUtils.actorName(classOf[TestIngestor])))
            .mapTo[LookupResult]
        ) { i =>
          i.ingestors.size shouldBe 1
          i.ingestors(0).name shouldBe ActorUtils.actorName(
            classOf[TestIngestor]
          )
        }
      }
    }

    it("unregisters") {
      act ! UnregisterAll
      eventually {
        whenReady((registry ? FindAll).mapTo[LookupResult]) { i =>
          i.ingestors.size shouldBe 0
        }
      }
    }
  }
}
