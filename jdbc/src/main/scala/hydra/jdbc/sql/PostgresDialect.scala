package hydra.jdbc.sql

import java.sql.Types

import hydra.jdbc.sql.JdbcUtils.{getNonNullableUnionType, isNullableUnion}
import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.Schema.Type
import org.apache.avro.Schema.Type.{BYTES, UNION}
import org.apache.avro.{LogicalTypes, Schema}

/**
  * Created by alexsilva on 5/4/17.
  */
private[sql] object PostgresDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:postgresql")

  override def getJDBCType(schema: Schema): Option[JdbcType] = schema.getType match {
    case Type.STRING => Some(JdbcType("TEXT", Types.CHAR))
    case BYTES => bytesType(schema)
    case Type.BOOLEAN => Some(JdbcType("BOOLEAN", Types.BOOLEAN))
    case Type.FLOAT => Some(JdbcType("FLOAT4", Types.FLOAT))
    case Type.DOUBLE => Some(JdbcType("FLOAT8", Types.DOUBLE))
    case UNION => unionType(schema)
    case Type.ARRAY =>
      getJDBCType(schema.getElementType).map(_.databaseTypeDefinition)
        .orElse(JdbcUtils.getCommonJDBCType(schema.getElementType).map(_.databaseTypeDefinition))
        .map(typeName => JdbcType(s"$typeName[]", java.sql.Types.ARRAY))
    case _ => None
  }

  private def unionType(schema: Schema): Option[JdbcType] = {
    if (isNullableUnion(schema)) {
      getJDBCType(getNonNullableUnionType(schema))
    } else {
      throw new IllegalArgumentException(s"Only nullable unions of two elements are supported.")
    }
  }

  private def bytesType(schema: Schema): Option[JdbcType] = {
    if (JdbcUtils.isLogicalType(schema, "decimal")) {
      val lt = LogicalTypes.fromSchema(schema).asInstanceOf[Decimal]
      Option(JdbcType(s"DECIMAL(${lt.getPrecision},${lt.getScale})", java.sql.Types.DECIMAL))
    } else {
      throw new IllegalArgumentException(s"Unsupported type in postgresql: ${schema.getType}")
    }
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(true)

}

