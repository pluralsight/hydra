package hydra.auth.persistence

import com.typesafe.config.ConfigFactory
import hydra.auth.persistence.TokenInfoRepository.TokenInfo
import hydra.common.logging.LoggingAdapter
import hydra.core.persistence.{FlywaySupport, H2PersistenceComponent}
import org.joda.time.DateTime
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.util.Try

class TokenRepositoryISpec extends FlatSpec
  with Matchers
  with BeforeAndAfterAll
  with ScalaFutures
  with LoggingAdapter
  with RepositoryModels {

  implicit val ec = scala.concurrent.ExecutionContext.global

  val persistenceDelegate = new H2PersistenceComponent(ec)

  val db = persistenceDelegate.db

  private val expectedTokenInfo = TokenInfo("test-token", Set("resourceA", "resourceB"))

  override def beforeAll(): Unit = {
    FlywaySupport.migrate(ConfigFactory.load().getConfig("db"))
    whenReady(db.run(tokenTable += (1, DateTime.now(), DateTime.now(), "test-token", 1))) { _ =>
      log.info("Data inserted into database")
    }
  }

  override def afterAll(): Unit = {
    Try(db.close()).recover{case _ => log.warn("Unable to shut down database")}
  }

  "A TokenRepository" should "retrieve token info from a storage backend" in {
    val tokenInfoRepo = new TokenInfoRepository(persistenceDelegate)

    whenReady(tokenInfoRepo.getByToken(expectedTokenInfo.token)) { actualTokenInfo =>
      actualTokenInfo shouldEqual expectedTokenInfo
    }
  }
}
