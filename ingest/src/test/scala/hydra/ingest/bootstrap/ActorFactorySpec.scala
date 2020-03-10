package hydra.ingest.bootstrap

import akka.actor.Props
import hydra.core.bootstrap.ServiceProvider
import hydra.ingest.test.TestIngestorDefault
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

class ActorFactorySpec extends Matchers with AnyFlatSpecLike {

  "The ActorFactory object" should "load actor Props from the classpath" in {
    ActorFactory
      .getActors().map(_._1) should contain allOf ("test1","test2")
  }
}

private class DummyServiceProvider extends ServiceProvider {
  override val services = Seq("test1" -> Props[TestIngestorDefault])
}

private object DummyServiceProviderObject extends ServiceProvider {
  override val services = Seq("test2" -> Props[TestIngestorDefault])
}
