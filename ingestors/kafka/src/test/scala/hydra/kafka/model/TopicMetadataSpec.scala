package hydra.kafka.model

import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.IO
import hydra.core.marshallers._
import hydra.kafka.model.TopicMetadataV2Request.Subject
import org.apache.avro.generic.{GenericDatumReader, GenericRecordBuilder}
import org.apache.avro.io.DecoderFactory
import org.scalatest.{FlatSpec, Matchers}
import vulcan.Codec

final class TopicMetadataSpec extends FlatSpec with Matchers {

  private def testCodec[A: Codec](a: A): Unit = {
    val schema = Codec[A].schema.toOption.get
    val encode: A => Any = Codec[A].encode(_).toOption.get
    val decode: Any => A =
      Codec[A]
        .decode(_, schema)
        .toOption
        .get
    it must s"encode and decode $a" in {
      decode(encode(a)) shouldBe a
    }
  }

  import TopicMetadataV2Value._
  List(Notification, CurrentState, History, Telemetry).map(
    testCodec[StreamType]
  )
  List(
    Public,
    InternalUseOnly,
    ConfidentialPII,
    RestrictedFinancial,
    RestrictedEmployeeData
  ).map(testCodec[DataClassification])

  val createdDate = Instant.now

  it must "encode TopicMetadataV2 key and value" in {
    val key = TopicMetadataV2Key(Subject.createValidated("test_subject").get)
    val value = TopicMetadataV2Value(
      History,
      false,
      Public,
      NonEmptyList.of(ContactMethod.create("test@test.com").get),
      createdDate,
      List.empty,
      None
    )

    val (encodedKey, encodedValue) =
      TopicMetadataV2.encode[IO](key, value).unsafeRunSync()

    encodedKey shouldBe new GenericRecordBuilder(
      TopicMetadataV2Key.codec.schema.toOption.get
    ).set("subject", key.subject.value)
      .build()

    val valueSchema = TopicMetadataV2Value.codec.schema.toOption.get

    val json =
      s"""{
         |"streamType":"History",
         |"deprecated": false,
         |"dataClassification":"Public",
         |"contact":[
         |  {
         |    "hydra.kafka.model.ContactMethod.Email": {"address": "test@test.com"}
         |  }
         |],
         |"createdDate":"${createdDate.toString}",
         |"parentSubjects": [],
         |"notes": null
         |}""".stripMargin

    val decoder = DecoderFactory.get().jsonDecoder(valueSchema, json)
    val valueRecord =
      new GenericDatumReader[Any](valueSchema).read(null, decoder)

    encodedValue shouldBe valueRecord
  }

  it must "encode and decode metadataV2" in {
    val key = TopicMetadataV2Key(Subject.createValidated("test_subject").get)
    val value = TopicMetadataV2Value(
      History,
      false,
      Public,
      NonEmptyList.of(ContactMethod.create("test@test.com").get),
      createdDate,
      List.empty,
      None
    )

    val (encodedKey, encodedValue) =
      TopicMetadataV2.encode[IO](key, value).unsafeRunSync()

    TopicMetadataV2Key.codec
      .decode(encodedKey, TopicMetadataV2Key.codec.schema.toOption.get)
      .toOption
      .get shouldBe key
    TopicMetadataV2Value.codec
      .decode(encodedValue, TopicMetadataV2Value.codec.schema.toOption.get)
      .toOption
      .get shouldBe value
  }
}
