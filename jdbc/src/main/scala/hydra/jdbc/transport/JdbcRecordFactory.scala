package hydra.jdbc.transport

import com.pluralsight.hydra.avro.JsonConverter
import hydra.core.avro.AvroValidation
import hydra.core.avro.registry.ConfluentSchemaRegistry
import hydra.core.avro.schema.SchemaResourceLoader
import hydra.core.ingest.RequestParams.HYDRA_SCHEMA_PARAM
import hydra.core.ingest.HydraRequest
import hydra.core.transport.RecordFactory
import org.apache.avro.generic.GenericRecord

import scala.util.Try

/**
  * Created by alexsilva on 5/19/17.
  */
object JdbcRecordFactory extends RecordFactory[String, GenericRecord] with ConfluentSchemaRegistry
  with AvroValidation {

  lazy val schemaResourceLoader = new SchemaResourceLoader(registryUrl, registry)

  private val ex = new IllegalArgumentException(s"No schema ${HYDRA_SCHEMA_PARAM} defined in the request.")

  def build(request: HydraRequest): Try[JdbcRecord] = {
    Try(schemaResourceLoader.getResource(getSubject(request)))
      .map(s => request.metadataValue("hydra-jdbc-table").getOrElse(s.schema.getName) -> s.schema)
      .map(r => r._1 -> new JsonConverter[GenericRecord](r._2).convert(request.payload))
      .map(a => JdbcRecord(a._1, a._2, request.retryStrategy, request.metadataValue("hydra-jdbc-primary-key")))
  }

  def getSubject(request: HydraRequest): String = {
    request.metadataValue(HYDRA_SCHEMA_PARAM).getOrElse(throw ex)
  }
}
