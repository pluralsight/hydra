package hydra.sql

import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/4/17.
  */
class JdbcDialectsSpec extends Matchers with FunSpecLike {

  describe("The JDBCDialects Object") {

    it("registers and unregisters a dialect") {
      JdbcDialects.registerDialect(PostgresDialect)
      JdbcDialects.registeredDialects should contain(PostgresDialect)
      JdbcDialects.unregisterDialect(PostgresDialect)
      JdbcDialects.registeredDialects should not contain (PostgresDialect)
    }

    it("gets by url") {
      JdbcDialects.registerDialect(PostgresDialect)
      JdbcDialects.registerDialect((url) => url.startsWith("jdbc:test"))
      JdbcDialects.registerDialect((url) => url.startsWith("jdbc:test"))
      JdbcDialects.get("jdbc:postgresql") shouldBe PostgresDialect
      JdbcDialects.get("jdbc:what") shouldBe NoopDialect
      JdbcDialects.get("jdbc:test").getClass shouldBe classOf[AggregatedDialect]
    }
  }
}
