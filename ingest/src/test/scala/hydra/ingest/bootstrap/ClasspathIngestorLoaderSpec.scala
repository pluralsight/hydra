package hydra.ingest.bootstrap

import hydra.ingest.test.TestIngestor
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/26/17.
  */
class ClasspathIngestorLoaderSpec extends Matchers with FunSpecLike {

  describe("When scanning the classpath for ingestors") {
    it("loads ingestors from a package") {
      new ClasspathIngestorLoader(Seq("hydra.ingest.test")).ingestors shouldBe Seq(classOf[TestIngestor])
    }

    it("requires at least one package") {
      intercept[IllegalArgumentException] {
        new ClasspathIngestorLoader(Seq.empty).ingestors
      }
    }
  }
}
