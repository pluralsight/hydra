package hydra.sql

import java.sql.Connection

/**
  * Created by alexsilva on 7/13/17.
  */
trait JdbcHelper {

  private[sql] def withConnection[T](conn: => Connection)(body: Connection => T): T = synchronized {
    val r = conn
    try {
      body(r)
    }
    finally {
      r.close()
    }
  }
}
