package hydra.sql

import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

/**
  * Created by alexsilva on 5/4/17.
  */
class JdbcDialectsSpec extends Matchers with AnyFunSpecLike {

  describe("The JDBCDialects Object") {

    it("registers and unregisters a dialect") {
      JdbcDialects.registerDialect(PostgresDialect)
      JdbcDialects.registeredDialects should contain(PostgresDialect)
      JdbcDialects.unregisterDialect(PostgresDialect)
      JdbcDialects.registeredDialects should not contain (PostgresDialect)
    }

    it("gets by url") {
      JdbcDialects.registerDialect(PostgresDialect)
      def tdialect = new JdbcDialect {
        override def canHandle(url: String): Boolean =
          url.startsWith("jdbc:test")
      }
      JdbcDialects.registerDialect(tdialect)
      JdbcDialects.registerDialect(tdialect)
      JdbcDialects.get("jdbc:postgresql") shouldBe PostgresDialect
      JdbcDialects.get("jdbc:what") shouldBe NoopDialect
      JdbcDialects.get("jdbc:test").getClass shouldBe classOf[AggregatedDialect]
    }
  }
}
