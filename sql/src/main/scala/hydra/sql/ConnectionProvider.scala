package hydra.sql

import java.sql.{Connection, DriverManager, SQLException}

import com.typesafe.config.Config
import com.zaxxer.hikari.HikariDataSource
import configs.syntax._
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Failure, Success, Try}

trait ConnectionProvider {
  def connectionUrl: String

  def getConnection(): Connection

  def close():Unit
}

/**
  * @param connectionUrl
  * @param username
  * @param password
  * @param maxConnectionAttempts
  * @param retryBackoff The time to wait following an error before a retry attempt is made
  */
class DriverManagerConnectionProvider private[sql](val connectionUrl: String,
                                                   val username: String,
                                                   val password: String,
                                                   val maxConnectionAttempts: Int = 3,
                                                   val retryBackoff: FiniteDuration = 3.seconds)
  extends ConnectionProvider {

  import DriverManagerConnectionProvider._

  private[sql] var connection: Connection = _

  def getConnection(): Connection = synchronized {
    if (connection == null) {
      doConnect()
    }
    else if (!connection.isValid(VALIDITY_CHECK_TIMEOUT)) {
      log.info("The database connection is invalid. Reconnecting...")
      closeQuietly()
      doConnect()
    }

    connection
  }

  override def close(): Unit = closeQuietly()

  private def doConnect(): Unit = {

    @annotation.tailrec
    def retry[T](n: Int)(fn: => T): T = {
      Try(fn) match {
        case Success(x) => x
        case Failure(e) if n > 1 =>
          log.info(s"Unable to connect to database. Will retry in $retryBackoff", e)
          Thread.sleep(retryBackoff.toMillis)
          retry(n - 1)(fn)
        case Failure(e) => throw e
      }
    }

    retry(maxConnectionAttempts) {
      log.debug(s"Attempting to connect to {}", connectionUrl)
      connection = DriverManager.getConnection(connectionUrl, username, password)
    }
  }

  def closeQuietly(): Unit = {
    Try(Option(connection).map(_.close())).recover {
      case e: SQLException => log.warn("Ignoring error closing connection", e)
    }
  }
}

object DriverManagerConnectionProvider {
  private val log = LoggerFactory.getLogger(classOf[ConnectionProvider])
  private val VALIDITY_CHECK_TIMEOUT = 5 // timeout in seconds

  def apply(config: Config): DriverManagerConnectionProvider = {
    new DriverManagerConnectionProvider(
      config.getString("connection.url"),
      config.get[String]("connection.user").valueOrElse(""),
      config.get[String]("connection.password").valueOrElse(""),
      config.get[Int]("connection.max.retries").valueOrElse(3),
      config.get[FiniteDuration]("connection.retry.backoff ").valueOrElse(3.seconds))
  }
}

class DataSourceConnectionProvider(ds: HikariDataSource) extends ConnectionProvider {

  override val connectionUrl = Option(ds.getJdbcUrl)
    .getOrElse(ds.getDataSourceProperties.getProperty("url"))

  override def getConnection(): Connection = ds.getConnection

  override def close(): Unit = ds.close()
}