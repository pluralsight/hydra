package hydra.core.connect

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import hydra.core.transport.{AckStrategy, ValidationStrategy}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

class ConnectorSettingSpec extends TestKit(ActorSystem("test"))
  with Matchers
  with FlatSpecLike
  with BeforeAndAfterAll {

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  
  "ConnectorSettings" should "read from configuration" in {
    val config = ConfigFactory.parseString(
      """
        |request.timeout = 2s
        |request.metadata {
        |   test = true
        |}
        |hydra-validation = relaxed
        |hydra-ack="replicated"
        |charset = "test"
        |
      """.stripMargin)
    val settings = new ConnectorSettings(config, system)
    settings.clustered shouldBe false //read from system
    settings.requestTimeout shouldBe 2.seconds
    settings.ackStrategy shouldBe AckStrategy.Replicated
    settings.validationStrategy shouldBe ValidationStrategy.Relaxed
    settings.charset shouldBe "test"
    settings.metadata shouldBe Map("test" -> "true")
  }

  it should "have sensible defaults in" in {
    val settings = new ConnectorSettings(ConfigFactory.empty(), system)
    settings.clustered shouldBe false
    settings.requestTimeout shouldBe 2.seconds
    settings.ackStrategy shouldBe AckStrategy.NoAck
    settings.validationStrategy shouldBe ValidationStrategy.Strict
    settings.charset shouldBe "UTF-8"
    settings.metadata shouldBe Map.empty
  }
}
