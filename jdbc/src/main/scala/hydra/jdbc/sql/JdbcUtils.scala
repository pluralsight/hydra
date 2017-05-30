package hydra.jdbc.sql

import java.sql.Connection

import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.Schema.Type
import org.apache.avro.Schema.Type._
import org.apache.avro.{LogicalTypes, Schema}

/**
  * Created by alexsilva on 5/4/17.
  */
private[sql] object JdbcUtils {

  import scala.collection.JavaConverters._

  val nullSchema = Schema.create(Type.NULL)

  def getCommonJDBCType(dt: Schema): Option[JdbcType] = {
    dt.getType match {
      case INT => commonIntTypes(dt)
      case BYTES => commonByteTypes(dt)
      case UNION => commonUnionTypes(dt)
      case _ => numberTypes(dt)
    }
  }


  private def commonIntTypes(schema: Schema): Option[JdbcType] = {
    if (isLogicalType(schema, "date")) {
      Option(JdbcType("DATE", java.sql.Types.DATE))
    } else {
      Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
    }
  }

  private def commonByteTypes(schema: Schema): Option[JdbcType] = {
    if (isLogicalType(schema, "decimal")) {
      val lt = LogicalTypes.fromSchema(schema).asInstanceOf[Decimal]
      Option(JdbcType(s"DECIMAL(${lt.getPrecision},${lt.getScale})", java.sql.Types.DECIMAL))
    }
    else {
      Option(JdbcType("BYTE", java.sql.Types.TINYINT))
    }
  }

  private def commonUnionTypes(dt: Schema): Option[JdbcType] = dt.getType match {
    case UNION if (isNullableUnion(dt)) => getCommonJDBCType(getNonNullableUnionType(dt))
    case _ => throw new IllegalArgumentException(s"Only nullable unions of two elements are supported.")
  }

  private def numberTypes(dt: Schema): Option[JdbcType] = dt.getType match {
    case LONG if isLogicalType(dt, "timestamp-millis") =>
      Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
    case LONG => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
    case DOUBLE => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
    case FLOAT => Option(JdbcType("REAL", java.sql.Types.FLOAT))
    case BOOLEAN => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
    case STRING => Option(JdbcType("TEXT", java.sql.Types.VARCHAR))
    case _ => None
  }


  def isLogicalType(schema: Schema, name: String) = {
    Option(schema.getLogicalType).map(_.getName == name) getOrElse (false)
  }

  def getNonNullableUnionType(schema: Schema): Schema = {
    if (schema.getTypes.get(0).getType == Type.NULL) schema.getTypes.get(1) else schema.getTypes.get(0)
  }

  def isNullableUnion(schema: Schema): Boolean =
    schema.getType == Type.UNION && schema.getTypes.size == 2 && schema.getTypes().contains(nullSchema)

  /**
    * Creates a table with a given schema.
    */
  def createTable(
                   schema: Schema,
                   url: String,
                   table: String,
                   createTableOptions: String,
                   conn: Connection): Unit = {
    val strSchema = schemaString(schema, url)
    val sql = s"CREATE TABLE $table ($strSchema) $createTableOptions"
    val statement = conn.createStatement
    try {
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }
  }

  def schemaString(schema: Schema, url: String): String = {
    val sb = new StringBuilder()
    val dialect = JdbcDialects.get(url)
    schema.getFields.asScala foreach { field =>
      val name = dialect.quoteIdentifier(field.name)
      val typ: String = getJdbcType(field.schema(), dialect).databaseTypeDefinition
      val nullable = if (isNullableUnion(field.schema())) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  def columns(schema: Schema): Seq[String] = {
    schema.getFields.asScala.map(_.name())
  }

  private def getJdbcType(schema: Schema, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(schema).orElse(getCommonJDBCType(schema)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${schema.getName}"))
  }
}
