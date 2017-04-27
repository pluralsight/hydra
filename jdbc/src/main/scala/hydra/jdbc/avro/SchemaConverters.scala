package hydra.jdbc.avro

import java.sql.JDBCType

import org.apache.avro.Schema
import org.apache.avro.Schema._

import scala.collection.mutable

/**
  * Created by alexsilva on 4/25/17.
  */

object SchemaConverters {

  private val AVRO_TYPE_JDBC_TYPE_MAPPING = Map(
    Type.BOOLEAN -> JDBCType.BOOLEAN,
    Type.INT -> JDBCType.INTEGER,
    Type.LONG -> JDBCType.BIGINT,
    Type.FLOAT -> JDBCType.FLOAT,
    Type.DOUBLE -> JDBCType.DOUBLE,
    Type.STRING -> JDBCType.VARCHAR,
    Type.ENUM -> JDBCType.VARCHAR)

  private val AVRO_SUPPORTED_TYPES = AVRO_TYPE_JDBC_TYPE_MAPPING.keys.toSeq :+ Type.UNION

  private val JDBC_SUPPORTED_TYPES = AVRO_TYPE_JDBC_TYPE_MAPPING.values.toSeq ++ Seq(
    JDBCType.DATE,
    JDBCType.TIME,
    JDBCType.TIMESTAMP)


//  def toDDL(inputSchema: Schema) = {
//    val avroColumnType = flatten(inputSchema);
//    String jsonStr = Preconditions.checkNotNull(workUnit.getProp(CONVERTER_AVRO_JDBC_DATE_FIELDS));
//    java.lang.reflect.Type typeOfMap = new TypeToken < Map < String
//    , JdbcType >> () {}.getType();
//    Map < String
//    , JdbcType > dateColumnMapping = new Gson().fromJson(jsonStr, typeOfMap);
//    LOG.info("Date column mapping: " + dateColumnMapping);
//
//    List < JdbcEntryMetaDatum > jdbcEntryMetaData = Lists.newArrayList();
//    for (Map.Entry < String
//    , Type > avroEntry: avroColumnType.entrySet
//    ()
//    )
//    {
//      String colName = tryConvertColumn(avroEntry.getKey(), this.avroToJdbcColPairs);
//      JdbcType JdbcType = dateColumnMapping.get(colName);
//      if (JdbcType == null) {
//        JdbcType = AVRO_TYPE_JDBC_TYPE_MAPPING.get(avroEntry.getValue());
//      }
//      Preconditions.checkNotNull(JdbcType, "Failed to convert " + avroEntry + " AVRO_TYPE_JDBC_TYPE_MAPPING: "
//        + AVRO_TYPE_JDBC_TYPE_MAPPING + " , dateColumnMapping: " + dateColumnMapping);
//      jdbcEntryMetaData.add(new JdbcEntryMetaDatum(colName, JdbcType));
//    }
//
//    JdbcEntrySchema converted = new JdbcEntrySchema(jdbcEntryMetaData);
//    LOG.info("Converted schema into " + converted);
//    return converted;
//  }
//
//  private static String tryConvertColumn(String key, Optional < Map < String, String >> mapping) {
//    if (!mapping.isPresent()) {
//      return key;
//    }
//
//    String converted = mapping.get().get(key);
//    return converted != null ? converted: key;
//  }
//
//
//  import org.apache.avro.Schema.Type
//
//  private def flatten(schema: Schema): Map[String, Type] = {
//    if (!(Type.RECORD == schema.getType))
//      throw new SchemaConversionException(s"${Type.RECORD} should be the first level element in the schema.")
//
//    val flattened = new mutable.HashMap[String, Type]
//
//    for (f <- schema.getFields) {
//      produceFlattenedHelper(f.schema, f, flattened)
//    }
//    flattened
//  }
//
//
//  private def produceFlattenedHelper(schema: Schema, field: Field):Option[Type] = {
//    if (Type.RECORD.equals(schema.getType))
//      throw new SchemaConversionException(s"${Type.RECORD} is only allowed for first level.")
//
//    inferType(schema)
//
//    inferType(schema) match {
//      case Some(t) =>
//        val existing = flattened.put(field.name, t)
//        if (existing != null) {
//          throw new SchemaConversionException("Duplicate name detected in Avro schema. " + field.name)
//        }
//      case None => throw new SchemaConversionException("Duplicate name detected in Avro schema. " + field.name)
//    }
//
//  }
//
//  private def inferType(schema: Schema): Option[Type] = {
//    import scala.collection.JavaConverters._
//    require(AVRO_SUPPORTED_TYPES.contains(schema.getType()), schema.getType() + " is not supported")
//
//    val inferred = schema.getType match {
//      case Type.UNION =>
//        val schemas = schema.getTypes
//        require(schemas.size() <= 2, s"More than two types for a union is not supported: $schemas")
//        schemas.asScala.filterNot(_.getType() == Type.NULL).headOption.map(_.getType)
//      case other => Some(other)
//
//    }
//
//    inferred
//  }

}

case class SchemaConversionException(msg: String) extends RuntimeException(msg)