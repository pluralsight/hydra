package hydra.kafka.config

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser

import scala.io.Source

object MetadataSchemaConfig {

  private def parseSchema(fileName: String): Schema = {
    val jsonSchemaString: String = Source.fromResource(fileName).mkString
    new Parser().parse(jsonSchemaString)
  }

  lazy val keySchema: Schema = parseSchema("HydraMetadataTopicKeyV2.avsc")

  lazy val valueSchema: Schema = parseSchema("HydraMetadataTopicValueV2.avsc")

}
