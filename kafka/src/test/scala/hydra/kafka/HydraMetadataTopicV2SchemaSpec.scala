package hydra.kafka

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.apache.avro.Schema.Parser
import scala.io.Source
import org.apache.avro.Schema

class HydraMetadataTopicV2SchemaSpec extends FlatSpec with Matchers {

    it should "be able to parse the HydraMetadataTopicValueV2.avsc into the Schema class" in {
        val jsonSchemaString: String = Source.fromResource("HydraMetadataTopicValueV2.avsc").mkString
        val schema = new Parser().parse(jsonSchemaString)
        schema shouldBe a[Schema]
    }

    it should "be able to parse the HydraMetadataTopicKeyV2.avsc into the Schema Class" in {
        val jsonSchemaString: String = Source.fromResource("HydraMetadataTopicKeyV2.avsc").mkString
        val schema = new Parser().parse(jsonSchemaString)
        schema shouldBe a[Schema]
    }

}