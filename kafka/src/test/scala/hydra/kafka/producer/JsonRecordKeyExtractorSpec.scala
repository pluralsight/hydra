package hydra.kafka.producer

import com.fasterxml.jackson.databind.ObjectMapper
import hydra.core.ingest
import hydra.core.ingest.RequestParams
import hydra.kafka.producer.KafkaRecordFactory.RecordKeyExtractor.JsonRecordKeyExtractor
import org.scalatest.{FlatSpecLike, Matchers}

class JsonRecordKeyExtractorSpec extends Matchers with FlatSpecLike {
  val mapper = new ObjectMapper()

  "The JsonRecordKeyExtractor" should "return none when no key is present" in {
    val json = """{"name":"hydra","rank":1}"""
    val node = mapper.reader().readTree(json)
    val request = ingest.HydraRequest("corr", node.asText())
    JsonRecordKeyExtractor.extractKey(request, node) shouldBe None
  }

  it should "return a key" in {
    val json = """{"name":"hydra","rank":1}"""
    val node = mapper.reader().readTree(json)
    val request = ingest.HydraRequest("corr", node.asText())
      .withMetadata(RequestParams.HYDRA_RECORD_KEY_PARAM -> "{$.name}")
    JsonRecordKeyExtractor.extractKey(request, node) shouldBe Some("hydra")
  }
}
