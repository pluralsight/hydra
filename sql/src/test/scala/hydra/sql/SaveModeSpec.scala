package hydra.sql

import hydra.avro.io.SaveMode
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/4/17.
  */
class SaveModeSpec extends Matchers with FunSpecLike {

  describe("SaveMode") {

    it("parses") {
      SaveMode.withName("Append") shouldBe SaveMode.Append
      SaveMode.withName("Ignore") shouldBe SaveMode.Ignore
    }
  }
}
