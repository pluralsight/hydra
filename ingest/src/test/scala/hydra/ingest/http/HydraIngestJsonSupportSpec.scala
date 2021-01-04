package hydra.ingest.http

import akka.actor.ActorPath
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import hydra.core.HydraException
import hydra.core.ingest.IngestionReport
import hydra.core.protocol.{IngestorCompleted, IngestorError, IngestorStatus, InvalidRequest}
import hydra.core.transport.ValidationStrategy
import hydra.ingest.IngestorInfo
import hydra.ingest.services.IngestionFlowV2.V2IngestRequest
import org.joda.time.DateTime
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

class HydraIngestJsonSupportSpec
    extends Matchers
    with AnyFunSpecLike
    with HydraIngestJsonSupport
    with SprayJsonSupport {

  import spray.json._

  describe("Hydra Json Support") {
    it("converts V2 Ingest Request") {
      val ingestRequest = V2IngestRequest("""{"one":1}""", None, Some(ValidationStrategy.Strict), useSimpleJsonFormat = false)
      ingestRequest.toJson.convertTo[V2IngestRequest] shouldBe ingestRequest
    }

    it("converts IngestorInfo objects") {
      val time = DateTime.now
      val info = IngestorInfo(
        "test",
        "test",
        ActorPath.fromString("akka://hydra/test/ingestor"),
        time
      )
      val expectedValue =
        s"""{"name":"test","group":"test","path":"akka://hydra/test/ingestor",
          "registeredAt":${time.toJson}}""".parseJson
      info.toJson shouldBe expectedValue
    }

    it("converts IngestorStatus objects") {
      val st = InvalidRequest(new IllegalArgumentException("error"))
        .asInstanceOf[IngestorStatus]
      val stn = InvalidRequest(new IllegalArgumentException())
        .asInstanceOf[IngestorStatus]
      st.toJson shouldBe """{"code":400,"message":"error"}""".parseJson
      stn.toJson shouldBe """{"code":400,"message":"Unknown error."}""".parseJson
      intercept[NotImplementedError] {
        """{"code":400,"message":"error"}""".parseJson.convertTo[IngestorStatus]
      }
    }

    it("converts IngestorError objects with no message") {
      val st = IngestorError(new IllegalArgumentException("error"))
        .asInstanceOf[IngestorStatus]
      val stn = IngestorError(new IllegalArgumentException(""))
        .asInstanceOf[IngestorStatus]
      val stnCause = IngestorError(
        new HydraException("hydra", new IllegalArgumentException("underlying"))
      ).asInstanceOf[IngestorStatus]
      st.toJson shouldBe """{"code":503,"message":"error"}""".parseJson
      stn.toJson shouldBe """{"code":503,"message":""}""".parseJson
      stnCause.toJson shouldBe """{"code":503,"message":"hydra: underlying"}""".parseJson
    }

    it("converts IngestionReport objects") {
      val report =
        IngestionReport("a123", Map("testIngestor" -> IngestorCompleted), 200)
      val json = report.toJson.asJsObject.fields

      val pjson =
        """{"correlationId":"a123","ingestors":{"testIngestor":{"code":200,"message":"OK"}}}""".parseJson.asJsObject.fields

      json("correlationId") shouldBe pjson("correlationId")
      json("ingestors") shouldBe pjson("ingestors")

      intercept[NotImplementedError] {
        """{"correlationId":"1","ingestors":{"testIngestor":{"code":200,
          "message":"OK"}}}""".parseJson.convertTo[IngestionReport]
      }
    }

    it("converts IngestionReport without any ingestors") {
      val report = IngestionReport("1", Map.empty, 200)
      val json = report.toJson.asJsObject.fields

      val pjson =
        """{"correlationId":"1","ingestors":{}}""".parseJson.asJsObject.fields

      json("correlationId") shouldBe pjson("correlationId")
      json("ingestors") shouldBe pjson("ingestors")

    }
  }

}
