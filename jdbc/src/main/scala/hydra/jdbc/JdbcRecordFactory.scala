package hydra.jdbc

import com.pluralsight.hydra.avro.JsonConverter
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.avro.resource.{SchemaResource, SchemaResourceLoader}
import hydra.avro.util.AvroUtils
import hydra.common.config.ConfigSupport
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.HYDRA_SCHEMA_PARAM
import hydra.core.transport.ValidationStrategy.Strict
import hydra.core.transport.{HydraRecord, RecordFactory, RecordMetadata}
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.GenericRecord

import scala.util.Try

object JdbcRecordFactory extends RecordFactory[Seq[Field], GenericRecord] with ConfigSupport {

  val schemaRegistry = ConfluentSchemaRegistry.forConfig(applicationConfig)

  val PRIMARY_KEY_PARAM = "hydra-db-primary-key"

  val TABLE_PARAM = "hydra-dtable"

  val DB_PROFILE_PARAM = "hydra-db-profile"

  lazy val schemaResourceLoader = new SchemaResourceLoader(schemaRegistry.registryUrl, schemaRegistry.registryClient)

  override def build(request: HydraRequest): Try[JdbcRecord] = {
    val schemaResource: Try[SchemaResource] = Try {
      val subject = request.metadataValue(HYDRA_SCHEMA_PARAM)
        .getOrElse(throw new IllegalArgumentException(s"A schema name is required [${HYDRA_SCHEMA_PARAM}]."))
      schemaResourceLoader.getResource(subject)
    }

    schemaResource.flatMap { s =>
      val converter = new JsonConverter[GenericRecord](s.schema, request.validationStrategy == Strict)
      Try(converter.convert(request.payload))
        .flatMap(rec => buildRecord(request, rec, s.schema))
        .recoverWith { case ex => throw schemaResource.map(r => AvroUtils.improveException(ex, r)).getOrElse(ex) }
    }
  }

  private[jdbc] def pk(request: HydraRequest, schema: Schema): Seq[Field] = {
    request.metadataValue(PRIMARY_KEY_PARAM).map(_.split(",")) match {
      case Some(ids) => ids.map(AvroUtils.getField(_, schema))
      case None => AvroUtils.getPrimaryKeys(schema)
    }
  }

  private def buildRecord(request: HydraRequest, record: GenericRecord, schema: Schema): Try[JdbcRecord] = {
    Try {
      val table = request.metadataValue(TABLE_PARAM).getOrElse(schema.getName)

      val dbProfile = request.metadataValue(DB_PROFILE_PARAM)
        .getOrElse(throw new IllegalArgumentException(s"A db profile name is required ${DB_PROFILE_PARAM}]."))

      JdbcRecord(table, Some(pk(request, schema)), record, dbProfile)
    }
  }
}

/**
  *
  * @param destination The Table name
  * @param key         The name of the primary key for the table (optional)
  * @param payload     The avro record representing a row in the table.
  */
case class JdbcRecord(destination: String, key: Option[Seq[Field]], payload: GenericRecord, dbProfile: String)
  extends HydraRecord[Seq[Field], GenericRecord]

case class JdbcRecordMetadata(table:String, timestamp: Long = System.currentTimeMillis) extends RecordMetadata