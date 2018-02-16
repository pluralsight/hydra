package hydra.avro

import com.pluralsight.hydra.avro.JsonToAvroConversionException
import org.apache.avro.Schema

/**
 * Created by alexsilva on 3/8/17.
 */
case class JsonToAvroConversionExceptionWithMetadata(cause: JsonToAvroConversionException, res: Schema)
  extends RuntimeException(cause) {
  override def getMessage: String = s"${super.getMessage} [${res.getFullName}]"
}
