package hydra.auth.persistence

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import hydra.auth.util.TokenGenerator
import org.flywaydb.core.Flyway
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global

// TODO add h2 support for test, create any necessary abstractions like in streams
class TokenRepositoryISpec extends FlatSpec
  with Matchers
  with BeforeAndAfterAll
  with ScalaFutures {

  val expectedTokenInfo = TokenGenerator.generateTokenInfo

  lazy val pg = EmbeddedPostgres.start()

  lazy val pgDb = pg.getPostgresDatabase()

  override def beforeAll() = {
    Flyway
      .configure()
      .dataSource()
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
