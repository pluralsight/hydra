package hydra.core.persistence

import com.typesafe.config.Config
import configs.syntax._
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory

object FlywaySupport {

  private val flogger = LoggerFactory.getLogger(getClass)

  def migrate(config: Config): Unit = {
    val urlConfig = config.get[String]("properties.url").toOption
    val userConfig = config.get[String]("properties.user").toOption
    val passwordConfig = config.get[String]("properties.password").toOption
    val migrateLocations = config.getString("flyway.locations")
    (urlConfig, userConfig, passwordConfig) match {
      case (Some(url), Some(user), Some(password)) =>
        Flyway
          .configure()
          .locations(migrateLocations)
          .dataSource(url, user, password)
          .load()
          .migrate()

      case _ => flogger.debug("Won't migrate database: Not configured properly; " +
        "url, user and password are required.")
    }
  }
}
