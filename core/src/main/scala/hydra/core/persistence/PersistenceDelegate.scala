package hydra.core.persistence

import slick.jdbc.JdbcBackend.Database

import scala.concurrent.ExecutionContext

trait PersistenceDelegate extends ProfileComponent with DatabaseComponent

trait PgPersistence extends PersistenceDelegate {
  val pgDb = Database.forConfig("pg-db")

  val pgProfile = slick.jdbc.PostgresProfile

  implicit val profile = pgProfile
  implicit val db: Database = pgDb

}

class PostgresPersistenceComponent(ec: ExecutionContext) extends PgPersistence