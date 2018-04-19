package hydra.sql

import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 5/4/17.
  */
class DataSourceConnectionProviderSpec extends Matchers
  with FlatSpecLike
  with BeforeAndAfterAll {

  val properties = new Properties
  val cfg = ConfigFactory.load().getConfig("db-cfg")
  cfg.entrySet().asScala
    .foreach(e => properties.setProperty(e.getKey(), cfg.getString(e.getKey())))

  private val hikariConfig = new HikariConfig(properties)

  private val ds = new HikariDataSource(hikariConfig)

  override def afterAll() = ds.close()

  "The DataSourceConnectionProvider" should "establish a connection" in {
    val p = new DataSourceConnectionProvider(ds)
    p.getConnection().isValid(1) shouldBe true
  }

  "The DriverManagerConnectionProvider" should "be configured properly" in {
    val config = ConfigFactory.parseString(
      """
        |connection.url = url
        |connection.user = test
        |connection.password = password
        |connection.max.retries = 20
        |connection.retry.backoff = 10s
        |
        |db.syntax = hydra.sql.NoOpSyntax
        |batch.size = 10
        |auto.evolve= true
        |
      """.stripMargin)

    val c = DriverManagerConnectionProvider(config)
    c.password shouldBe "password"
    c.connectionUrl shouldBe "url"
    c.username shouldBe "test"
    c.retryBackoff.toSeconds shouldBe 10
    c.maxConnectionAttempts shouldBe 20

  }
}
