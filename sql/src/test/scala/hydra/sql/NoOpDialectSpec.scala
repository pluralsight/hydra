package hydra.sql

import org.apache.avro.Schema
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/4/17.
  */
class NoOpDialectSpec extends Matchers with FunSpecLike {

  describe("The NoOp dialect") {
    it("handles everything") {
      NoopDialect.canHandle("url") shouldBe true
    }

    it("does not upsert") {
      intercept[UnsupportedOperationException] {
        NoopDialect.buildUpsert("table", Schema.create(Schema.Type.NULL), UnderscoreSyntax)
      }
    }

    it("returns the correct json placeholder") {
      NoopDialect.jsonPlaceholder shouldBe "?"
    }
  }
}
