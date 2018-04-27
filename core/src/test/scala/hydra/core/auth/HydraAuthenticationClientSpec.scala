package hydra.core.auth

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class HydraAuthenticationClientSpec extends TestKit(ActorSystem("hydra-auth-client"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "A HydraAuthenticationClient" should
    "return the user name on a valid request" in {
    fail()
  }


}
