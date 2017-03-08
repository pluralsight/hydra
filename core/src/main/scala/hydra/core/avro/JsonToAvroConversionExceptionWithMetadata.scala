package hydra.core.avro

import com.pluralsight.hydra.avro.JsonToAvroConversionException
import hydra.core.avro.schema.SchemaResource

/**
  * Created by alexsilva on 3/8/17.
  */
case class JsonToAvroConversionExceptionWithMetadata(cause: JsonToAvroConversionException, res: SchemaResource)
  extends RuntimeException(cause) {
  override def getMessage: String = s"${super.getMessage} [${res.location}]"
}
