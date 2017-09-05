package hydra.core.akka

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class ComposeReceiveSpec extends TestKit(ActorSystem("test")) with Matchers with
  FlatSpecLike with BeforeAndAfterAll with ImplicitSender {

  override def afterAll = TestKit.shutdownActorSystem(system)

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