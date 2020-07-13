package hydra.kafka.model

import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.IO
import hydra.kafka.model.TopicMetadataV2Request.Subject
import org.apache.avro.generic.{GenericDatumReader, GenericRecordBuilder}
import org.apache.avro.io.DecoderFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike
import vulcan.Codec

final class TopicMetadataSpec extends AnyFlatSpecLike with Matchers {

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
  List(StreamTypeV2.Event, StreamTypeV2.Entity, StreamTypeV2.Telemetry).map(
    testCodec[StreamTypeV2]
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
      StreamTypeV2.Entity,
      false,
      Public,
      NonEmptyList.of(ContactMethod.create("test@test.com").get),
      createdDate,
      List.empty,
      None
    )

    val (encodedKey, encodedValue) =
      TopicMetadataV2.encode[IO](key, Some(value)).unsafeRunSync()

    encodedKey shouldBe new GenericRecordBuilder(
      TopicMetadataV2Key.codec.schema.toOption.get
    ).set("subject", key.subject.value)
      .build()

    val valueSchema = TopicMetadataV2Value.codec.schema.toOption.get

    val json =
      s"""{
         |"streamType":"Entity",
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

    encodedValue shouldBe Some(valueRecord)
  }

  it must "encode and decode metadataV2" in {
    val key = TopicMetadataV2Key(Subject.createValidated("test_subject").get)
    val value = TopicMetadataV2Value(
      StreamTypeV2.Entity,
      false,
      Public,
      NonEmptyList.of(ContactMethod.create("test@test.com").get),
      createdDate,
      List.empty,
      None
    )

    val (encodedKey, encodedValue) =
      TopicMetadataV2.encode[IO](key, Some(value)).unsafeRunSync()

    val (decodedKey,decodedValue) =
      TopicMetadataV2.decode[IO](encodedKey, encodedValue).unsafeRunSync()

    decodedKey shouldBe key
    decodedValue shouldBe Some(value)
  }
}
