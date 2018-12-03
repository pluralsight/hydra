package hydra.sql

import java.sql.JDBCType

import hydra.avro.convert.IsoDate
import hydra.avro.util.SchemaWrapper
import hydra.common.logging.LoggingAdapter
import hydra.sql.JdbcUtils.isLogicalType
import org.apache.avro.Conversions.UUIDConversion
import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.Schema.Type.{BYTES, UNION}
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.{LogicalTypes, Schema}

/**
  * Created by alexsilva on 5/4/17.
  */
private[sql] object PostgresDialect extends JdbcDialect with LoggingAdapter {

  override val jsonPlaceholder = "to_json(?::json)"

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:postgresql")

  private val uc = new UUIDConversion

  override def getJDBCType(schema: Schema): Option[JdbcType] = schema.getType match {
    case Type.STRING => logicalStringTypes(schema)
    case BYTES => bytesType(schema)
    case Type.BOOLEAN => Some(JdbcType("BOOLEAN", JDBCType.BOOLEAN))
    case Type.FLOAT => Some(JdbcType("FLOAT4", JDBCType.FLOAT))
    case Type.DOUBLE => Some(JdbcType("FLOAT8", JDBCType.DOUBLE))
    case UNION => unionType(schema)
    case Type.RECORD => Some(JdbcType("JSON", JDBCType.VARCHAR))
    case Type.ARRAY => getArrayType(schema)
    case _ => None
  }

  private def logicalStringTypes(schema: Schema) = {
    if (isLogicalType(schema, IsoDate.IsoDateLogicalTypeName)) {
      Some(JdbcType("TIMESTAMP", JDBCType.TIMESTAMP))
    } else if (isLogicalType(schema, uc.getLogicalTypeName)) {
      Some(JdbcType("UUID", JDBCType.OTHER))
    }
    else {
      Some(JdbcType("TEXT", JDBCType.VARCHAR))
    }
  }

  override def getArrayType(schema: Schema): Option[JdbcType] = {
    getJDBCType(schema.getElementType).map(_.databaseTypeDefinition)
      .orElse(JdbcUtils.getCommonJDBCType(schema.getElementType).map(_.databaseTypeDefinition))
      .map { typeName =>
        val arrayType = if (typeName == "JSON") "JSON" else s"$typeName[]" //we store json arrays as JSON as well
        JdbcType(arrayType, java.sql.JDBCType.ARRAY)
      }
  }

  private def unionType(schema: Schema): Option[JdbcType] = {
    if (JdbcUtils.isNullableUnion(schema)) {
      getJDBCType(JdbcUtils.getNonNullableUnionType(schema))
    } else {
      throw new IllegalArgumentException(s"Only nullable unions of two elements are supported.")
    }
  }

  override def upsertFields(schema: SchemaWrapper): Seq[Field] = {
    schema.getFields
  }

  override def buildUpsert(table: String, schema: SchemaWrapper, dbs: DbSyntax): String = {
    def formatColName(col: Field) = quoteIdentifier(dbs.format(col.name()))

    val idFields = schema.primaryKeys.map(schema.schema.getField)
    val fields = schema.getFields
    val columns = fields.map(formatColName).mkString(",")
    val placeholders = parameterize(fields)
    val updateSchema = fields -- idFields
    val upsertColumns = updateSchema.map(formatColName).map(col => s"${col} = EXCLUDED.${col}")

    val sql =
      s"""insert into $table ($columns) values (${placeholders.mkString(",")})
         |on conflict (${idFields.map(formatColName).mkString(",")})
         |do update set ${upsertColumns.mkString(",")};""".stripMargin

    sql

  }

  private def bytesType(schema: Schema): Option[JdbcType] = {
    if (JdbcUtils.isLogicalType(schema, "decimal")) {
      val lt = LogicalTypes.fromSchema(schema).asInstanceOf[Decimal]
      Option(JdbcType(s"DECIMAL(${lt.getPrecision},${lt.getScale})", JDBCType.DECIMAL))
    } else {
      Option(JdbcType(s"BYTEA", JDBCType.BINARY))
    }
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(true)

  override def alterTableQueries(table: String, missingFields: Seq[Field], dbs: DbSyntax): Seq[String] = {
    missingFields.map { f =>
      val dbDef = JdbcUtils.getJdbcType(f.schema, this).databaseTypeDefinition
      val colName = quoteIdentifier(dbs.format(f.name))
      s"alter table $table add column $colName $dbDef"
    }
  }

  override def dropNotNullConstraintQueries(table: String, schema: SchemaWrapper, dbs: DbSyntax): Seq[String] = {
    schema.getFields.filterNot(f => schema.primaryKeys.contains(f.name)).map { f =>
      val colName = quoteIdentifier(dbs.format(f.name))
      val sql = s"alter table $table alter column $colName drop not null"
      log.debug(sql)
      sql
    }
  }

  override def tableNameForMetadataQuery(tableName: String): String = tableName.toLowerCase

}

