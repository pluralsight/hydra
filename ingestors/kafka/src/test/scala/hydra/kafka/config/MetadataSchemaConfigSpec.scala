package hydra.kafka.config

import org.apache.avro.Schema
import org.scalatest.{FlatSpec, Matchers}

class MetadataSchemaConfigSpec extends FlatSpec with Matchers {

    it should "be able to parse the HydraMetadataTopicValueV2.avsc into the Schema class" in {
        MetadataSchemaConfig.keySchema shouldBe a[Schema]
    }

    it should "be able to parse the HydraMetadataTopicKeyV2.avsc into the Schema Class" in {
        MetadataSchemaConfig.valueSchema shouldBe a[Schema]
    }

}