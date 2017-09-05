package hydra.core.avro

import com.pluralsight.hydra.avro.{JsonConverter, JsonToAvroConversionException}
import hydra.core.avro.schema.{SchemaResource, SchemaResourceLoader}
import hydra.core.ingest.HydraRequest
import hydra.core.protocol.{InvalidRequest, MessageValidationResult, ValidRequest}
import hydra.core.transport.ValidationStrategy.Strict
import org.apache.avro.generic.GenericRecord

import scala.util.Try

/**
  *
  * Created by alexsilva on 5/19/17.
  */
trait AvroValidation {

  def schemaResourceLoader: SchemaResourceLoader

  def getSubject(request: HydraRequest): String

  /**
    * Validates a hydra request against a schema.
    */
  def validate(request: HydraRequest): MessageValidationResult = {
    val schemaResource: Try[SchemaResource] = Try(schemaResourceLoader.getResource(getSubject(request)))
    schemaResource.flatMap { s =>
      val strict = request.validationStrategy == Strict
      val converter = new JsonConverter[GenericRecord](s.schema, strict)
      Try(converter.convert(request.payload)).map(_ => ValidRequest)
        .recover { case ex => InvalidRequest(schemaResource.map(r => improveException(ex, r)).getOrElse(ex)) }
    }.get
  }

  private def improveException(ex: Throwable, schemaResource: SchemaResource) = {
    ex match {
      case e: JsonToAvroConversionException => JsonToAvroConversionExceptionWithMetadata(e, schemaResource)
      case e: Exception => e
    }
  }
}
