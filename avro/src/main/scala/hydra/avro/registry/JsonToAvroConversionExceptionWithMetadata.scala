package hydra.avro.registry

import com.pluralsight.hydra.avro.JsonToAvroConversionException
import hydra.avro.resource.SchemaResource

/**
  * Created by alexsilva on 3/8/17.
  */
case class JsonToAvroConversionExceptionWithMetadata(
    cause: JsonToAvroConversionException,
    metadata: SchemaResource,
    registryUrl: String
) extends RuntimeException(cause) {
  val location = s"$registryUrl/schemas/ids/${metadata.id}"

  override def getMessage: String = s"${super.getMessage} [$location]"
}
