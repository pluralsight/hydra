package hydra.ingest.bootstrap

import hydra.ingest.test.{TestIngestor, TestTransport}
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/26/17.
  */
class ClasspathHydraComponentLoaderSpec extends Matchers with FunSpecLike {

  describe("When scanning the classpath for ingestors") {
    it("loads ingestors from a package") {
      new ClasspathHydraComponentLoader(Seq("hydra.ingest.test")).ingestors shouldBe Seq(classOf[TestIngestor])
    }

    it("loads transports from a package") {
      new ClasspathHydraComponentLoader(Seq("hydra.ingest.test")).transports shouldBe Seq(classOf[TestTransport])
    }

    it("requires at least one package") {
      intercept[IllegalArgumentException] {
        new ClasspathHydraComponentLoader(Seq.empty).ingestors
      }
    }
  }
}
