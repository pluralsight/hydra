package hydra.core.persistence

import slick.jdbc.JdbcBackend.Database

trait DatabaseComponent {
  val db: Database
}