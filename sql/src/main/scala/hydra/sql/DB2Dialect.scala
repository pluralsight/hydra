package hydra.sql

import java.sql.JDBCType

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._

/**
  * Created by alexsilva on 5/4/17.
  */
private object DB2Dialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:db2")

  override def getJDBCType(dt: Schema): Option[JdbcType] = dt.getType match {
    case STRING => Option(JdbcType("CLOB", JDBCType.CLOB))
    case BOOLEAN => Option(JdbcType("CHAR(1)", JDBCType.CHAR))
    case _ => None
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

}
