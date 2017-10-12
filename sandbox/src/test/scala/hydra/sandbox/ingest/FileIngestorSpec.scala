package hydra.sandbox.ingest

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import hydra.common.config.ConfigSupport
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class FileIngestorSpec extends TestKit(ActorSystem("hydra-sandbox-test")) with Matchers with FunSpecLike
  with ImplicitSender with ConfigSupport with BeforeAndAfterAll {

  override def afterAll = TestKit.shutdownActorSystem(system)


}
