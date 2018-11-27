package hydra.common.auth

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import hydra.common.util.TryWith
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

class TokenRepositoryISpec extends FlatSpec
  with Matchers
  with BeforeAndAfterAll
  with ScalaFutures {

  val expectedTokenInfo = TokenGenerator.generateTokenInfo

  lazy val pg = EmbeddedPostgres.start()

  lazy val pgDb = pg.getPostgresDatabase()

  override def beforeAll() = {
    TryWith(pgDb.getConnection("postgres", "")) { conn =>
      val createStmt = conn.createStatement

      val insertStmt = conn.createStatement

      try {
        val ddl = Source.fromResource("token_info.sql").mkString
        createStmt.execute(ddl)
      } finally {
        createStmt.close()
      }

      try {
        insertStmt.execute(
          s"""INSERT INTO token_info (token, insert_date, groups)
             | VALUES ('${expectedTokenInfo.token}', '1999-12-31T12:58:35Z', '{"GroupA", "GroupB"}')"""
            .stripMargin)
      } finally {
        insertStmt.close()
      }
    }.get
  }

  override def afterAll(): Unit = {
    pg.close()
  }

  "A TokenRepository" should "retrieve token info from a storage backend" in {
    val tokenInfoRepo = new TokenInfoRepository

    whenReady(tokenInfoRepo.getByToken(expectedTokenInfo.token)) { actualTokenInfo =>
      actualTokenInfo shouldEqual expectedTokenInfo
    }
  }
}



class HydraTokenException(msg: String) extends Exception