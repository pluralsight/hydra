package hydra.common.auth

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures

class HydraAuthenticatorFactorySpec extends TestKit(ActorSystem("HydraAuthenticatorFactory"))
  with Matchers
  with FlatSpecLike
  with ScalaFutures
  with BeforeAndAfterAll {

  "HydraAuthenticatorFactory" should
    "throw ClassNotFoundException for an invalid authenticator when auth is enabled" in {
      val authenticatorClassName = ""
      intercept[ClassNotFoundException] {
        HydraAuthenticatorFactory(true, Some(authenticatorClassName))
    }
  }

  it should "throw a ClassNotFoundException when auth is enabled and no authenticator is passed in" in {
    intercept[ClassNotFoundException] {
      HydraAuthenticatorFactory(true, None)
    }
  }

  it should
    "Instantiate a NoSecurityAuthenticator hydra authenticator when auth is enabled and a valid authenticator is passed in" in {
      val authenticatorClassName = "hydra.common.auth.NoSecurityAuthenticator"
      val authenticator = HydraAuthenticatorFactory(true, Some(authenticatorClassName))
      authenticator.get.getClass shouldBe classOf[NoSecurityAuthenticator]
  }

  it should "Return None when a valid authenticator is passed in and auth is NOT enabled" in {
    val authenticatorClassName = "hydra.common.auth.NoSecurityAuthenticator"
    val authenticator = HydraAuthenticatorFactory(false, Some(authenticatorClassName))
    authenticator shouldBe None
  }

  it should "Return None when an invalid authenticator is passed in and auth is NOT enabled" in {
    val authenticatorClassName = ""
    val authenticator = HydraAuthenticatorFactory(false, Some(authenticatorClassName))
    authenticator shouldBe None
  }

  it should "Return None when authenticator is NOT passed in and auth is NOT enabled" in {
    val authenticator = HydraAuthenticatorFactory(false, None)
    authenticator shouldBe None
  }
}
