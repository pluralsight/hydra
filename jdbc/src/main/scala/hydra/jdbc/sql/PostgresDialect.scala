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
    case BYTES if schema.getLogicalType!=null && schema.getLogicalType.getName == "decimal" =>
      val lt = LogicalTypes.fromSchema(schema).asInstanceOf[Decimal]
      Option(JdbcType(s"DECIMAL(${lt.getPrecision},${lt.getScale})", java.sql.Types.DECIMAL))
    case Type.BYTES => Some(JdbcType("BYTEA", Types.BINARY))
    case Type.BOOLEAN => Some(JdbcType("BOOLEAN", Types.BOOLEAN))
    case Type.FLOAT => Some(JdbcType("FLOAT4", Types.FLOAT))
    case Type.DOUBLE => Some(JdbcType("FLOAT8", Types.DOUBLE))
    case UNION if (isNullableUnion(schema)) => getJDBCType(getNonNullableUnionType(schema))
    case UNION => throw new IllegalArgumentException(s"Only nullable unions of two elements are supported.");
    case _ => JdbcUtils.getCommonJDBCType(schema)
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }


  override def isCascadingTruncateTable(): Option[Boolean] = Some(true)
}

