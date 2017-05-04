package hydra.jdbc.sql

import org.apache.avro.Schema

private class AggregatedDialect(dialects: List[JdbcDialect]) extends JdbcDialect {

  require(dialects.nonEmpty)

  override def canHandle(url: String): Boolean =
    dialects.map(_.canHandle(url)).reduce(_ && _)


  override def getJDBCType(dt: Schema): Option[JdbcType] = {
    dialects.flatMap(_.getJDBCType(dt)).headOption
  }
}
