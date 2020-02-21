package hydra.avro.registry

import com.pluralsight.hydra.avro.JsonToAvroConversionException
import hydra.avro.registry.JsonToAvroConversionExceptionWithMetadata.registryUrl
import hydra.avro.resource.SchemaResource
import hydra.common.config.ConfigSupport

/**
  * Created by alexsilva on 3/8/17.
  */
case class JsonToAvroConversionExceptionWithMetadata(
    cause: JsonToAvroConversionException,
    metadata: SchemaResource
) extends RuntimeException(cause) {
  val location = s"$registryUrl/schemas/ids/${metadata.id}"

  override def getMessage: String = s"${super.getMessage} [$location]"
}

object JsonToAvroConversionExceptionWithMetadata extends ConfigSupport {
  val registryUrl = ConfluentSchemaRegistry.registryUrl(applicationConfig)
}
