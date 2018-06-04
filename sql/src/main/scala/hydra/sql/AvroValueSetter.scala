package hydra.sql

import java.math.{MathContext, RoundingMode}
import java.nio.ByteBuffer
import java.sql.{PreparedStatement, Timestamp}
import java.time.{LocalDate, ZoneId}

import com.google.common.collect.Lists
import hydra.avro.convert.{ISODateConverter, IsoDate}
import hydra.avro.util.SchemaWrapper
import hydra.sql.JdbcUtils.{getJdbcType, isLogicalType}
import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.Schema.Field
import org.apache.avro.Schema.Type.LONG
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{AvroRuntimeException, LogicalTypes, Schema}

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 7/12/17.
  */
//scalastyle:off
private[sql] class AvroValueSetter(schema: SchemaWrapper, dialect: JdbcDialect) {

  private val pk = schema.primaryKeys

  private val fields = if (pk.isEmpty) schema.getFields else dialect.upsertFields(schema)

  val fieldTypes: Map[Field, JdbcType] = fields.map { field =>
    field -> getJdbcType(field.schema(), dialect)
  }.toMap

  def bind(schema: Schema, fields: Map[Field, AnyRef], stmt: PreparedStatement) = {
    fields.zipWithIndex.foreach {
      case (f, idx) =>
        setFieldValue(f._2, fieldTypes(f._1), schema, stmt, idx + 1)
    }

    stmt.addBatch()
  }

  def bind(record: GenericRecord, stmt: PreparedStatement) = {
    fields.zipWithIndex.foreach {
      case (f, idx) =>
        setFieldValue(record.get(f.name()), fieldTypes(f), f.schema(), stmt, idx + 1)
    }

    stmt.addBatch()
  }

  private def setFieldValue(value: AnyRef, jdbcType: JdbcType,
                            schema: Schema, pstmt: PreparedStatement, idx: Int): Unit = {
    if (value == null) {
      pstmt.setNull(idx, jdbcType.targetSqlType.getVendorTypeNumber.intValue())
    } else {
      schema.getType match {
        case Schema.Type.UNION => unionValue(value, jdbcType, schema, pstmt, idx)
        case Schema.Type.ARRAY =>
          value match {
            case a: GenericData.Array[_] => arrayValue(a.iterator().asScala.toList, schema, pstmt, idx)
            case l: java.util.List[_] => arrayValue(l.asScala.toList, schema, pstmt, idx)
          }
        case Schema.Type.STRING if isLogicalType(schema, IsoDate.LogicalTypeName) =>
          pstmt.setTimestamp(idx,
            new Timestamp(new ISODateConverter()
              .fromCharSequence(value.toString, schema, IsoDate).toInstant.toEpochMilli))
        case Schema.Type.STRING =>
          pstmt.setString(idx, value.toString)
        case Schema.Type.BOOLEAN =>
          pstmt.setBoolean(idx, value.asInstanceOf[Boolean])
        case Schema.Type.DOUBLE =>
          pstmt.setDouble(idx, value.toString.toDouble: java.lang.Double)
        case Schema.Type.FLOAT => pstmt.setFloat(idx, value.toString.toFloat)
        case Schema.Type.INT if isLogicalType(schema, "date") =>
          val ld = LocalDate.ofEpochDay(value.toString.toInt)
          //todo: time zones?
          val inst = ld.atStartOfDay(ZoneId.systemDefault()).toInstant()
          pstmt.setDate(idx, new java.sql.Date(inst.toEpochMilli))
        case Schema.Type.INT => pstmt.setInt(idx, value.toString.toInt)
        case LONG if isLogicalType(schema, "timestamp-millis") =>
          pstmt.setTimestamp(idx, new Timestamp(value.toString.toLong))
        case Schema.Type.LONG => pstmt.setLong(idx, value.toString.toLong)
        case Schema.Type.BYTES => byteValue(value, schema, pstmt, idx)
        case Schema.Type.ENUM => pstmt.setString(idx, value.toString)
        case Schema.Type.RECORD => pstmt.setString(idx, value.toString)
        case Schema.Type.NULL =>
          pstmt.setNull(idx, jdbcType.targetSqlType.getVendorTypeNumber.intValue())
        case _ => throw new IllegalArgumentException(s"Type ${schema.getType} is not supported.")
      }
    }
  }

  private def byteValue(obj: AnyRef, schema: Schema, pstmt: PreparedStatement, idx: Int) = {
    if (isLogicalType(schema, "decimal")) {
      val lt = LogicalTypes.fromSchema(schema).asInstanceOf[Decimal]
      val ctx = new MathContext(lt.getPrecision, RoundingMode.HALF_EVEN)
      val decimal = new java.math.BigDecimal(obj.toString, ctx).setScale(lt.getScale)
      pstmt.setBigDecimal(idx, decimal)
    }
    else {
      pstmt.setBytes(idx, obj.asInstanceOf[ByteBuffer].array())
    }
  }

  private[sql] def arrayValue(values: List[_], schema: Schema, pstmt: PreparedStatement, idx: Int): Unit = {
    val aType = JdbcUtils.getJdbcType(schema.getElementType, dialect)
    if (aType.databaseTypeDefinition == "JSON") {
      //if it is json, we don't insert it as an array.
      val elems = Lists.newArrayList(values.map(_.toString): _*).toArray
      val colVal = "[" + elems.mkString(",") + "]"
      pstmt.setString(idx, colVal)
    } else {
      val arr = pstmt.getConnection.createArrayOf(aType.targetSqlType.getName, values
        .asInstanceOf[List[AnyRef]].toArray)
      pstmt.setArray(idx, arr)
    }


  }

  private def unionValue(obj: AnyRef, jdbcType: JdbcType, schema: Schema, pstmt: PreparedStatement, idx: Int): Unit = {
    val types = schema.getTypes

    if (!JdbcUtils.isNullableUnion(schema)) {
      throw new AvroRuntimeException("Unions may only consist of a concrete type and null in hydra avro.")
    }
    if (types.size == 1) {
      setFieldValue(obj, jdbcType, types.get(0), pstmt, idx)
    }
    else setFieldValue(obj, jdbcType, JdbcUtils.getNonNullableUnionType(schema), pstmt, idx)
  }
}
