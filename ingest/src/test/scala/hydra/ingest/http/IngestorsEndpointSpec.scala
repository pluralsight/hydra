package hydra.ingest.http

import akka.actor.Actor
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import hydra.common.util.ActorUtils
import hydra.ingest.IngestorInfo
import hydra.ingest.services.IngestorRegistry.{FindAll, FindByName, LookupResult}
import org.joda.time.DateTime
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

/**
  * Created by alexsilva on 5/12/17.
  */
class IngestorsEndpointSpec
    extends Matchers
    with AnyWordSpecLike
    with ScalatestRouteTest
    with HydraIngestJsonSupport {

  val ingestorsRoute = new IngestorRegistryEndpoint().route

  override def afterAll = {
    super.afterAll()
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10 seconds
    )
  }

  val probe = TestProbe()

  val ingestorInfo = IngestorInfo(
    ActorUtils.actorName(probe.ref),
    "test",
    probe.ref.path,
    DateTime.now
  )

  val registry = TestActorRef(
    new Actor {

      override def receive = {
        case FindByName("tester") => sender ! LookupResult(Seq(ingestorInfo))
        case FindAll              => sender ! LookupResult(Seq(ingestorInfo))
      }
    },
    "ingestor_registry"
  ).underlyingActor

  "The ingestors endpoint" should {

    "returns all ingestors" in {
      Get("/ingestors") ~> ingestorsRoute ~> check {
        val r = responseAs[Seq[IngestorInfo]]
        r.size shouldBe 1
        r(0).path shouldBe ingestorInfo.path
        r(0).group shouldBe ingestorInfo.group
        r(0).path shouldBe ingestorInfo.path
        r(0).registeredAt shouldBe ingestorInfo.registeredAt.withMillisOfSecond(
          0
        )
      }
    }
  }
}
