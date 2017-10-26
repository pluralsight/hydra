package hydra.sql

import org.apache.avro.Schema

import scala.util.Try

private class AggregatedDialect(dialects: List[JdbcDialect]) extends JdbcDialect {

  require(dialects.nonEmpty)

  override def canHandle(url: String): Boolean =
    dialects.map(_.canHandle(url)).reduce(_ && _)


  override def getJDBCType(dt: Schema): Option[JdbcType] = {
    dialects.flatMap(_.getJDBCType(dt)).headOption
  }

  override def buildUpsert(table: String, schema: Schema, dbs: DbSyntax): String = {
    dialects.map(d => Try(d.buildUpsert(table, schema, dbs))).head.get
  }
}
