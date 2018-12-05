package hydra.core.persistence


import slick.jdbc.JdbcBackend.Database

import scala.concurrent.ExecutionContext

trait H2Persistence extends PersistenceDelegate {
  val h2Db = Database.forConfig("h2-db")

  val h2Profile = slick.jdbc.H2Profile

  implicit val profile = h2Profile
  implicit val db: Database = h2Db
}

class H2PersistenceComponent(ec: ExecutionContext) extends H2Persistence