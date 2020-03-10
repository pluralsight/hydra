package hydra.sql

import hydra.avro.util.SchemaWrapper
import org.apache.avro.Schema
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

/**
  * Created by alexsilva on 5/4/17.
  */
class NoOpDialectSpec extends Matchers with AnyFunSpecLike {

  describe("The NoOp dialect") {
    it("handles everything") {
      NoopDialect.canHandle("url") shouldBe true
    }

    it("does not upsert") {
      intercept[UnsupportedOperationException] {
        NoopDialect.buildUpsert(
          "table",
          SchemaWrapper.from(Schema.create(Schema.Type.NULL)),
          UnderscoreSyntax
        )
      }
    }

    it("returns the correct json placeholder") {
      NoopDialect.jsonPlaceholder shouldBe "?"
    }

    it("does not support dropping constraints by default") {
      intercept[UnsupportedOperationException] {
        NoopDialect.dropNotNullConstraintQueries(
          "table",
          null,
          UnderscoreSyntax
        )
      }
    }
  }
}
