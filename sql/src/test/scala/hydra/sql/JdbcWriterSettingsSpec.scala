package hydra.sql

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpecLike, Matchers}

/**
  * Created by alexsilva on 7/14/17.
  */
class JdbcWriterSettingsSpec extends Matchers with FlatSpecLike {

  "The JdbcWriterSettings" should "be properly configured" in {
    val config = ConfigFactory.parseString(
      """
        |db.syntax = hydra.sql.NoOpSyntax
        |auto.evolve= true
        |
      """.stripMargin)

    val c = JdbcWriterSettings(config)
    c.dbSyntax shouldBe NoOpSyntax
    c.autoEvolve shouldBe true
  }

  it should "use defaults" in {
    val config = ConfigFactory.parseString(
      """
        |connection.url = url
        |connection.user = test
        |connection.password = password
        |dialect = hydra.sql.PostgresDialect
      """.stripMargin)

    val c = JdbcWriterSettings(config)
    c.autoEvolve shouldBe true
    c.batchSize shouldBe 3000
    c.dbSyntax shouldBe UnderscoreSyntax
    c.autoEvolve shouldBe true
  }

}
