package hydra.sql

import java.sql.JDBCType

import hydra.avro.util.AvroUtils
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 5/4/17.
  */
private object H2Dialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:h2")

  override def getJDBCType(dt: Schema): Option[JdbcType] = dt.getType match {
    case STRING => Option(JdbcType("CLOB", JDBCType.CLOB))
    case BOOLEAN => Option(JdbcType("CHAR(1)", JDBCType.CHAR))
    case _ => None
  }

  override def buildUpsert(table: String, schema: Schema, dbs: DbSyntax): String = {

    val idFields = AvroUtils.getPrimaryKeys(schema)
    val fields = schema.getFields.asScala
    val columns = fields.map(c => quoteIdentifier(dbs.format(c.name))).mkString(",")
    val placeholders = fields.map(_ => "?").mkString(",")
    val pk = idFields.map(i => quoteIdentifier(dbs.format(i.name))).mkString(",")
    val sql =
      s"""merge into ${table} ($columns) key($pk) values ($placeholders);"""
        .stripMargin
    sql
  }

  override def alterTableQueries(table: String, missingFields: Seq[Schema.Field], dbs: DbSyntax): Seq[String] = {
    missingFields.map { f =>
      val dbDef = JdbcUtils.getJdbcType(f.schema, this).databaseTypeDefinition
      val colName = quoteIdentifier(dbs.format(f.name))
      s"alter table $table add column $colName $dbDef"
    }
  }
}
