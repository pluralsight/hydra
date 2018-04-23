package hydra.sql

import java.sql.{Connection, JDBCType}

import hydra.avro.util.SchemaWrapper
import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.Schema.Type._
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.{LogicalTypes, Schema}
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created by alexsilva on 5/4/17.
  */
private[sql] object JdbcUtils {

  import scala.collection.JavaConverters._

  val nullSchema = Schema.create(Type.NULL)

  val logger = LoggerFactory.getLogger(getClass)

  def getCommonJDBCType(dt: Schema): Option[JdbcType] = {
    dt.getType match {
      case INT => commonIntTypes(dt)
      case BYTES => commonByteTypes(dt)
      case UNION => commonUnionTypes(dt)
      case ENUM => Some(JdbcType("TEXT", JDBCType.VARCHAR))
      case _ => numberTypes(dt)
    }
  }


  private def commonIntTypes(schema: Schema): Option[JdbcType] = {
    if (isLogicalType(schema, "date")) {
      Option(JdbcType("DATE", JDBCType.DATE))
    } else {
      Option(JdbcType("INTEGER", JDBCType.INTEGER))
    }
  }

  private def commonByteTypes(schema: Schema): Option[JdbcType] = {
    if (isLogicalType(schema, "decimal")) {
      val lt = LogicalTypes.fromSchema(schema).asInstanceOf[Decimal]
      Option(JdbcType(s"DECIMAL(${lt.getPrecision},${lt.getScale})", JDBCType.DECIMAL))
    }
    else {
      Option(JdbcType("BYTE", JDBCType.TINYINT))
    }
  }

  private def commonUnionTypes(dt: Schema): Option[JdbcType] = dt.getType match {
    case UNION if (isNullableUnion(dt)) => getCommonJDBCType(getNonNullableUnionType(dt))
    case _ => throw new IllegalArgumentException(s"Only nullable unions of two elements are supported.")
  }

  private def numberTypes(dt: Schema): Option[JdbcType] = dt.getType match {
    case LONG if isLogicalType(dt, "timestamp-millis") =>
      Option(JdbcType("TIMESTAMP", JDBCType.TIMESTAMP))
    case LONG => Option(JdbcType("BIGINT", JDBCType.BIGINT))
    case DOUBLE => Option(JdbcType("DOUBLE PRECISION", JDBCType.DOUBLE))
    case FLOAT => Option(JdbcType("REAL", JDBCType.FLOAT))
    case BOOLEAN => Option(JdbcType("BIT(1)", JDBCType.BIT))
    case STRING => Option(JdbcType("TEXT", JDBCType.VARCHAR))
    case _ => None
  }


  def isLogicalType(schema: Schema, name: String) = {
    Option(schema.getLogicalType).map(_.getName == name) getOrElse false
  }

  def getNonNullableUnionType(schema: Schema): Schema = {
    if (schema.getTypes.get(0).getType == Type.NULL) schema.getTypes.get(1) else schema.getTypes.get(0)
  }

  def isNullableUnion(schema: Schema): Boolean =
    schema.getType == Type.UNION && schema.getTypes.size == 2 && schema.getTypes().contains(nullSchema)

  /**
    * Creates a table with a given schema.
    */
  def createTable(schema: SchemaWrapper,
                  dialect: JdbcDialect,
                  table: String,
                  createTableOptions: String,
                  dbSyntax: DbSyntax,
                  conn: Connection): Int = {
    val strSchema = schemaString(schema, dialect, dbSyntax)
    val sql = s"CREATE TABLE $table ($strSchema) $createTableOptions"
    logger.debug(sql)
    val statement = conn.createStatement
    try {
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }
  }

  def createSchema(name: String, createSchemaOptions: String, conn: Connection): Int = {
    val sql = s"CREATE SCHEMA $name $createSchemaOptions"
    logger.debug(sql)
    val statement = conn.createStatement
    try {
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }
  }


  /**
    * Returns true if the table already exists in the JDBC database.
    */
  def tableExists(conn: Connection, dialect: JdbcDialect, table: String): Boolean = {
    Try {
      val sql = dialect.getTableExistsQuery(table)
      logger.info(sql)
      val statement = conn.prepareStatement(sql)
      try {
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  def schemaString(schema: SchemaWrapper, dialect: JdbcDialect,
                   dbSyntax: DbSyntax = NoOpSyntax): String = {
    val schemaStr = schema.getFields.map { field =>
      val name = dialect.quoteIdentifier(dbSyntax.format(field.name))
      val typ = getJdbcType(field.schema(), dialect).databaseTypeDefinition
      val nullable = if (isNullableUnion(field.schema())) "" else "NOT NULL"
      s"$name $typ $nullable"
    }
    val pkSeq = schema.primaryKeys
      .map(f => dialect.quoteIdentifier(dbSyntax.format(f.name())))
    val pkStmt = if (!pkSeq.isEmpty) s",CONSTRAINT ${schema.getName}_PK PRIMARY KEY (${pkSeq.mkString(",")})" else ""
    val ddl = s"${schemaStr.mkString(",")}${pkStmt}"
    ddl
  }


  def columnMap(fields: Seq[Field], dialect: JdbcDialect, dbSyntax: DbSyntax): Map[Schema.Field, Column] = {
    fields.map { field =>
      val name = dbSyntax.format(field.name)
      val typ = getJdbcType(field.schema(), dialect)
      val nullable = isNullableUnion(field.schema())
      field -> Column(name, field.schema(), typ, nullable, Option(field.doc()))
    }
      .toMap
  }

  def columns(schema: Schema, dialect: JdbcDialect, dbSyntax: DbSyntax = NoOpSyntax): Seq[Column] = {
    columnMap(schema, dialect, dbSyntax).values.toSeq
  }

  def columnMap(schema: Schema, dialect: JdbcDialect, dbSyntax: DbSyntax = NoOpSyntax): Map[Schema.Field, Column] =
    columnMap(schema.getFields.asScala, dialect, dbSyntax)

  def columnNames(schema: Schema, dbSyntax: DbSyntax = NoOpSyntax): Seq[String] = {
    schema.getFields.asScala.map(f => dbSyntax.format(f.name()))
  }

  def getJdbcType(schema: Schema, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(schema).orElse(getCommonJDBCType(schema)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${schema.getName}"))
  }
}
