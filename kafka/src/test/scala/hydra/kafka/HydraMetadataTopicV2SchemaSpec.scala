package hydra.kafka

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.apache.avro.Schema.Parser
import scala.io.Source
import org.apache.avro.Schema

class HydraMetadataTopicV2SchemaSpec extends FlatSpec with Matchers {

    it should "be able to parse the HydraMetadataTopicV2.avsc into the Schema class" in {
        val jsonSchemaString: String = Source.fromResource("HydraMetadataTopicV2.avsc").mkString
        new Parser().parse(jsonSchemaString) shouldBe a[Schema]
    } 

}