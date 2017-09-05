package hydra.ingest.services

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import hydra.ingest.ingestors.IngestorInfo
import hydra.ingest.services.IngestorRegistry._
import hydra.ingest.test.TestIngestor
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/9/17.
  */
class IngestorRegistrySpec extends TestKit(ActorSystem("hydra")) with Matchers
  with FunSpecLike with ImplicitSender with Eventually with BeforeAndAfterAll {

  override def afterAll = TestKit.shutdownActorSystem(system)

  val registry = system.actorOf(Props[IngestorRegistry], "registry")

  val listenerActor = system.actorOf(Props[ListenerTestActor])

  system.eventStream.subscribe(listenerActor, classOf[IngestorTerminated])

  describe("The ingestor registry") {
    it("registers an ingestor") {
      registry ! RegisterWithClass(classOf[TestIngestor], "global")
      expectMsgType[IngestorInfo]
    }
    it("does not allow duplicate names") {
      registry ! RegisterWithClass(classOf[TestIngestor], "global")
      expectMsgType[IngestorAlreadyRegistered]
    }
    it("allows same class to be registered under a different name") {
      registry ! RegisterWithClass(classOf[TestIngestor], "global", Some("test"))
      expectMsgType[IngestorInfo]
    }
    it("unregisters") {
      registry ! RegisterWithClass(classOf[TestIngestor], "global", Some("toDelete"))
      expectMsgType[IngestorInfo]

      registry ! Unregister("toDelete")
      expectMsgType[Unregistered]

      registry ! Unregister("123-test")
      expectMsgType[IngestorNotFound]
    }

    it("finds by name") {
      registry ! RegisterWithClass(classOf[TestIngestor], "global", Some("find"))
      expectMsgType[IngestorInfo]

      registry ! FindByName("find")
      expectMsgPF() {
        case LookupResult(i) =>
          i.size shouldBe 1
          i(0).name shouldBe "find"
      }
    }

    it("finds all") {
      registry ! RegisterWithClass(classOf[TestIngestor], "global", Some("find-all"))
      expectMsgType[IngestorInfo]

      registry ! FindAll
      expectMsgPF() {
        case LookupResult(i) => i.map(_.name) should contain("find-all")
      }
    }

    it("restarts by default") {
      val reg = TestActorRef[IngestorRegistry](Props[IngestorRegistry])
      val strategy = reg.underlyingActor.supervisorStrategy.decider
      strategy(new IllegalArgumentException) should be(Restart)
    }

    it("is notified of ingestor termination") {
      registry ! RegisterWithClass(classOf[TestIngestor], "global", Some("terminator"))
      expectMsgType[IngestorInfo]

      registry ! FindByName("terminator")
      expectMsgPF() {
        case LookupResult(i) =>
          i.size shouldBe 1
          i(0).name shouldBe "terminator"
          system.actorSelection("akka://hydra/user/registry/terminator") ! PoisonPill

          eventually {
            listenerActor ! "ingestor"
            expectMsg("akka://hydra/user/registry/terminator")
          }
      }
    }
  }
}

private class ListenerTestActor extends Actor {
  @volatile
  var ingestor: String = ""

  override def receive = {
    case IngestorTerminated(s) => ingestor = s
    case "ingestor" => sender ! ingestor
  }
}

