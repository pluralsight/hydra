package hydra.core.persistence

import slick.jdbc.JdbcBackend.Database

import scala.concurrent.ExecutionContext

trait PersistenceDelegate extends ProfileComponent with DatabaseComponent

trait PgPersistence extends PersistenceDelegate {
  implicit val profile = slick.jdbc.PostgresProfile

  implicit val db: Database = Database.forConfig("pg-db")

}

class PostgresPersistenceComponent(ec: ExecutionContext) extends PgPersistence