package hydra.kafka.serializers

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

/**
 * Created by alexsilva on 12/3/16.
 */
class JsonDecoder(props: VerifiableProperties = null) extends Decoder[JsonNode] {

  val encoding =
    if (props == null)
      "UTF8"
    else
      props.getString("serializer.encoding", "UTF8")

  val mapper = new ObjectMapper()

  override def fromBytes(bytes: Array[Byte]): JsonNode = {
    val json = new String(bytes, encoding)
    mapper.reader().readTree(json)
  }
}

