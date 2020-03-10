package hydra.common.logging

import akka.actor.{Actor, ActorSystem}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.BeforeAndAfterAll

/**
  * Created by alexsilva on 3/7/17.
  */
class LoggingAdapterSpec
    extends TestKit(ActorSystem("test"))
    with Matchers
    with AnyFunSpecLike
    with BeforeAndAfterAll {

  override def afterAll = TestKit.shutdownActorSystem(system)

  describe("The logging adapter") {

    it("allows an actor to use the logger") {

      val act = TestActorRef(new Actor with ActorLoggingAdapter {
        override def receive = {
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
