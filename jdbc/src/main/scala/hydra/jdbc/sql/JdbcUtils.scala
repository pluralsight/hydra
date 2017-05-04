package hydra.jdbc.sql

import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.Schema.Type
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.Schema.Type._

/**
  * Created by alexsilva on 5/4/17.
  */
private[sql] object JdbcUtils {

  def getCommonJDBCType(dt: Schema): Option[JdbcType] = {
    dateTypes(dt) orElse (numberTypes(dt)) orElse (unionTypes(dt)) orElse (defaultTypes(dt))
  }

  private val dateTypes: PartialFunction[Schema, Option[JdbcType]] = (dt: Schema) => dt.getType match {
    case INT if isLogicalType(dt, "date") => Option(JdbcType("DATE", java.sql.Types.DATE))
    case LONG if isLogicalType(dt, "timestamp-millis") => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
  }

  private def unionTypes(dt: Schema): Option[JdbcType] = dt.getType match {
    case UNION if (isNullableUnion(dt)) => getCommonJDBCType(getNonNullableUnionType(dt))
    case UNION => throw new IllegalArgumentException(s"Only nullable unions of two elements are supported.")
  }

  private def numberTypes(dt: Schema): Option[JdbcType] = dt.getType match {
    case INT => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
    case LONG => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
    case DOUBLE => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
    case FLOAT => Option(JdbcType("REAL", java.sql.Types.FLOAT))
    case BYTES if isLogicalType(dt, "decimal") =>
      val lt = LogicalTypes.fromSchema(dt).asInstanceOf[Decimal]
      Option(JdbcType(s"DECIMAL(${lt.getPrecision},${lt.getScale})", java.sql.Types.DECIMAL))
    case BYTES => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
  }

  private def defaultTypes(dt: Schema): Option[JdbcType] = dt.getType match {
    case BOOLEAN => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
    case STRING => Option(JdbcType("TEXT", java.sql.Types.CLOB))
    case _ => None
  }

  def isLogicalType(schema: Schema, name: String) = {
    Option(schema.getLogicalType).map(_.getName == name) getOrElse (false)
  }

  def getNonNullableUnionType(schema: Schema): Schema = {
    if (schema.getTypes.get(0).getType == Type.NULL) {
      schema.getTypes.get(1)
    }
    else {
      schema.getTypes.get(0)
    }
  }

  def isNullableUnion(schema: Schema) =
    schema.getType == Type.UNION &&
      schema.getTypes.size == 2 &&
      (schema.getTypes.get(0).getType == Type.NULL || schema.getTypes.get(1).getType == Type.NULL)
}
