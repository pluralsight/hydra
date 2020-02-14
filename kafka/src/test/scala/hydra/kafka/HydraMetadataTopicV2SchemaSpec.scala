package hydra.kafka

import hydra.kafka.config.MetadataSchemaConfig
import org.apache.avro.Schema
import org.scalatest.{FlatSpec, Matchers}

class HydraMetadataTopicV2SchemaSpec extends FlatSpec with Matchers {

    it should "be able to parse the HydraMetadataTopicValueV2.avsc into the Schema class" in {
        MetadataSchemaConfig.keySchema shouldBe a[Schema]
    }

    it should "be able to parse the HydraMetadataTopicKeyV2.avsc into the Schema Class" in {
        MetadataSchemaConfig.valueSchema shouldBe a[Schema]
    }

}