//package hydra.core.persistence
//
//package persistence
//
//import com.typesafe.config.ConfigFactory
//import hydra.streams.core.persistence.{FlywaySupport, PostgresPersistenceComponent}
//import org.scalatest.concurrent.ScalaFutures
//import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
//import util.PostgresTestSupport
//
//import scala.concurrent.ExecutionContext.global
//
//class FlywaySupportISpec extends FlatSpec
//  with Matchers
//  with ScalaFutures
//  with BeforeAndAfterAll
//  with PostgresTestSupport {
//
//  implicit val ec = global
//
//  val dbComponent = new PostgresPersistenceComponent(ec)
//
//  import dbComponent.profile.api._
//
//  val db = dbComponent.db
//
//  override def afterAll(): Unit = {
//    teardown(db)
//    whenReady(db.run(sqlu"TRUNCATE TABLE flyway_schema_history")) { r =>
//      r shouldBe 0
//    }
//
//    db.close()
//  }
//
//  "FlywaySupport" should "migrate a table" in {
//    FlywaySupport.migrate(
//      ConfigFactory
//        .load()
//        .getConfig("pg-db")
//    )
//
//    val countQuery = sql"""SELECT COUNT(*) FROM "HYDRA_JOBS"""".as[Int]
//
//    whenReady(db.run(countQuery)) { r =>
//      r.head shouldBe 0
//    }
//  }
//}
//
