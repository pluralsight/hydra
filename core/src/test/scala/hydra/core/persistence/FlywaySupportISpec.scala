package hydra.core.persistence

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.global

class FlywaySupportISpec extends FlatSpec
  with Matchers
  with ScalaFutures
  with BeforeAndAfterAll {

  implicit val ec = global

  val dbComponent = new H2PersistenceComponent(ec)

  import dbComponent.profile.api._

  val db = dbComponent.db

  override def afterAll(): Unit = {
    whenReady(db.run(sqlu"TRUNCATE TABLE flyway_schema_history")) { r =>
      r shouldBe 0
    }

    db.close()
  }

  "FlywaySupport" should "migration a table" in {
    FlywaySupport.migrate(
      ConfigFactory
        .load()
        .getConfig("h2-db")
    )

    val countQuery = sql"""SELECT COUNT(*) FROM test""".as[Int]

    whenReady(db.run(countQuery)) { r =>
      r.head shouldBe 0
    }
  }
}
