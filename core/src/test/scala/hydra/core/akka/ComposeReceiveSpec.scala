package hydra.core.akka

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

class ComposeReceiveSpec
    extends TestKit(ActorSystem("test"))
    with Matchers
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with ImplicitSender {

  override def afterAll =
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)

  "The ComposingReceiveTrait" should "compose" in {
    system.actorOf(Props[TestBaseActor]) ! "foo"
    expectMsg("bar")

    system.actorOf(Props[TestComposeActor]) ! "foo"
    expectMsg("new-bar")
  }

}

trait TestBase extends Actor with ComposingReceive {

  override def baseReceive = {
    case "foo" => sender ! "bar"
  }
}

class TestBaseActor extends TestBase {
  compose(Actor.emptyBehavior)
}

class TestComposeActor extends TestBase {
  compose {
    case "foo" => sender ! "new-bar"
  }
}
