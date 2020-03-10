package hydra.sql

import hydra.avro.io.SaveMode
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

/**
  * Created by alexsilva on 5/4/17.
  */
class SaveModeSpec extends Matchers with AnyFunSpecLike {

  describe("SaveMode") {

    it("parses") {
      SaveMode.withName("Append") shouldBe SaveMode.Append
      SaveMode.withName("Ignore") shouldBe SaveMode.Ignore
    }
  }
}
