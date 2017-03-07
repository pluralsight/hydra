package hydra.common.logging

import akka.actor.ActorDSL._
import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/7/17.
  */
class LoggingAdapterSpec extends TestKit(ActorSystem("test")) with Matchers with FunSpecLike {

  describe("The logging adapter") {

    it("allows an actor to use the logger") {

      val act = TestActorRef(new Act with ActorLoggingAdapter {
        become {
          case _ => log.info("got it"); sender ! "got it"
        }
      }, "logger-test")


      act.underlyingActor.log.getName shouldBe "akka.testkit.TestActorRef"

      // Send a message and make sure we get a response back
      val probe = TestProbe()
      probe.send(act, "test")
      probe.expectMsgType[String] shouldBe "got it"
    }
  }
}
