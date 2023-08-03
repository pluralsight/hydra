package hydra.kafka.model

import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.IO
import hydra.kafka.model.TopicMetadataV2Request.Subject
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityChecker
import org.apache.avro.Schema
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

  import TopicMetadataV2ValueOptionalTagList._
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
    val key = TopicMetadataV2Key(Subject.createValidated("dvs.test-subject").get)
    val value = TopicMetadataV2Value(
      StreamTypeV2.Entity,
      false,
      None,
      None,
      None,
      Public,
      NonEmptyList.of(ContactMethod.create("test@test.com").get),
      createdDate,
      List.empty,
      None,
      Some("dvs-teamName"),
      List.empty,
      None,
      None
    )

    val (encodedKey, encodedValue, headers) =
      TopicMetadataV2.encode[IO](key, Some(value)).unsafeRunSync()

    encodedKey shouldBe new GenericRecordBuilder(
      TopicMetadataV2Key.codec.schema.toOption.get
    ).set("subject", key.subject.value)
      .build()

    val valueSchema = TopicMetadataV2ValueOptionalTagList.codec.schema.toOption.get

    val json =
      s"""{
         |"streamType":"Entity",
         |"deprecated": false,
         |"deprecatedDate": null,
         |"replacementTopics": null,
         |"previousTopics": null,
         |"dataClassification":"Public",
         |"teamName":{"string":"dvs-teamName"},
         |"contact":[
         |  {
         |    "hydra.kafka.model.ContactMethod.Email": {"address": "test@test.com"}
         |  }
         |],
         |"createdDate":"${createdDate.toString}",
         |"parentSubjects": [],
         |"notes": null,
         |"notificationUrl": null,
         |"tags": null,
         |"_validations": null
         |}""".stripMargin

    val decoder = DecoderFactory.get().jsonDecoder(valueSchema, json)
    val valueRecord =
      new GenericDatumReader[Any](valueSchema).read(null, decoder)

    // String replacement due to DecoderFactory decoding differently
    encodedValue.toString shouldBe Some(valueRecord).toString.replace(""""tags": null""", """"tags": []""")
  }

  it must "encode and decode metadataV2" in {
    val key = TopicMetadataV2Key(Subject.createValidated("dvs.test-subject").get)
    val value = TopicMetadataV2Value(
      StreamTypeV2.Entity,
      false,
      None,
      None,
      None,
      Public,
      NonEmptyList.of(ContactMethod.create("test@test.com").get),
      createdDate,
      List.empty,
      None,
      Some("dvs-teamName"),
      List.empty,
      Some("notificationUrl"),
      None
    )

    val (encodedKey, encodedValue, headers) =
      TopicMetadataV2.encode[IO](key, Some(value)).unsafeRunSync()

    val (decodedKey,decodedValue) =
      TopicMetadataV2.decode[IO](encodedKey, encodedValue).unsafeRunSync()

    decodedKey shouldBe key
    decodedValue shouldBe Some(value)
  }

  "TopicMetadata" should "have compatible schema evolutions" in {
    import collection.JavaConverters._
    val schemaVersion1String = "{\"type\":\"record\",\"name\":\"TopicMetadataV2Value\",\"namespace\":\"_hydra.v2\",\"fields\":[{\"name\":\"streamType\",\"type\":{\"type\":\"enum\",\"name\":\"StreamTypeV2\",\"namespace\":\"hydra.kafka.model\",\"symbols\":[\"Event\",\"Entity\",\"Telemetry\"]}},{\"name\":\"deprecated\",\"type\":\"boolean\"},{\"name\":\"dataClassification\",\"type\":{\"type\":\"enum\",\"name\":\"DataClassification\",\"namespace\":\"hydra.kafka.model\",\"symbols\":[\"Public\",\"InternalUseOnly\",\"ConfidentialPII\",\"RestrictedFinancial\",\"RestrictedEmployeeData\"]}},{\"name\":\"contact\",\"type\":{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"Email\",\"namespace\":\"hydra.kafka.model.ContactMethod\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"}]},{\"type\":\"record\",\"name\":\"Slack\",\"namespace\":\"hydra.kafka.model.ContactMethod\",\"fields\":[{\"name\":\"channel\",\"type\":\"string\"}]}]}},{\"name\":\"createdDate\",\"type\":\"string\"},{\"name\":\"parentSubjects\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"notes\",\"type\":[\"null\",\"string\"]}]}"
    val schemaVersion2String = "{\"type\":\"record\",\"name\":\"TopicMetadataV2Value\",\"namespace\":\"_hydra.v2\",\"fields\":[{\"name\":\"streamType\",\"type\":{\"type\":\"enum\",\"name\":\"StreamTypeV2\",\"namespace\":\"hydra.kafka.model\",\"symbols\":[\"Event\",\"Entity\",\"Telemetry\"]}},{\"name\":\"deprecated\",\"type\":\"boolean\"},{\"name\":\"deprecatedDate\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"dataClassification\",\"type\":{\"type\":\"enum\",\"name\":\"DataClassification\",\"namespace\":\"hydra.kafka.model\",\"symbols\":[\"Public\",\"InternalUseOnly\",\"ConfidentialPII\",\"RestrictedFinancial\",\"RestrictedEmployeeData\"]}},{\"name\":\"contact\",\"type\":{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"Email\",\"namespace\":\"hydra.kafka.model.ContactMethod\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"}]},{\"type\":\"record\",\"name\":\"Slack\",\"namespace\":\"hydra.kafka.model.ContactMethod\",\"fields\":[{\"name\":\"channel\",\"type\":\"string\"}]}]}},{\"name\":\"createdDate\",\"type\":\"string\"},{\"name\":\"parentSubjects\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"notes\",\"type\":[\"null\",\"string\"]}]}"
    val schemaVersion3String = "{\"type\":\"record\",\"name\":\"TopicMetadataV2Value\",\"namespace\":\"_hydra.v2\",\"fields\":[{\"name\":\"streamType\",\"type\":{\"type\":\"enum\",\"name\":\"StreamTypeV2\",\"namespace\":\"hydra.kafka.model\",\"symbols\":[\"Event\",\"Entity\",\"Telemetry\"]}},{\"name\":\"deprecated\",\"type\":\"boolean\"},{\"name\":\"deprecatedDate\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"dataClassification\",\"type\":{\"type\":\"enum\",\"name\":\"DataClassification\",\"namespace\":\"hydra.kafka.model\",\"symbols\":[\"Public\",\"InternalUseOnly\",\"ConfidentialPII\",\"RestrictedFinancial\",\"RestrictedEmployeeData\"]}},{\"name\":\"contact\",\"type\":{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"Email\",\"namespace\":\"hydra.kafka.model.ContactMethod\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"}]},{\"type\":\"record\",\"name\":\"Slack\",\"namespace\":\"hydra.kafka.model.ContactMethod\",\"fields\":[{\"name\":\"channel\",\"type\":\"string\"}]}]}},{\"name\":\"createdDate\",\"type\":\"string\"},{\"name\":\"parentSubjects\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"notes\",\"type\":[\"null\",\"string\"]},{\"name\":\"teamName\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
    val schemaVersion4String = "{\"type\":\"record\",\"name\":\"TopicMetadataV2Value\",\"namespace\":\"_hydra.v2\",\"fields\":[{\"name\":\"streamType\",\"type\":{\"type\":\"enum\",\"name\":\"StreamTypeV2\",\"namespace\":\"hydra.kafka.model\",\"symbols\":[\"Event\",\"Entity\",\"Telemetry\"]}},{\"name\":\"deprecated\",\"type\":\"boolean\"},{\"name\":\"deprecatedDate\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"dataClassification\",\"type\":{\"type\":\"enum\",\"name\":\"DataClassification\",\"namespace\":\"hydra.kafka.model\",\"symbols\":[\"Public\",\"InternalUseOnly\",\"ConfidentialPII\",\"RestrictedFinancial\",\"RestrictedEmployeeData\"]}},{\"name\":\"contact\",\"type\":{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"Email\",\"namespace\":\"hydra.kafka.model.ContactMethod\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"}]},{\"type\":\"record\",\"name\":\"Slack\",\"namespace\":\"hydra.kafka.model.ContactMethod\",\"fields\":[{\"name\":\"channel\",\"type\":\"string\"}]}]}},{\"name\":\"createdDate\",\"type\":\"string\"},{\"name\":\"parentSubjects\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"notes\",\"type\":[\"null\",\"string\"]},{\"name\":\"teamName\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"tags\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"string\"}],\"default\":null}]}"
    val schemaVersion5String = "{\"type\":\"record\",\"name\":\"TopicMetadataV2Value\",\"namespace\":\"_hydra.v2\",\"fields\":[{\"name\":\"streamType\",\"type\":{\"type\":\"enum\",\"name\":\"StreamTypeV2\",\"namespace\":\"hydra.kafka.model\",\"symbols\":[\"Event\",\"Entity\",\"Telemetry\"]}},{\"name\":\"deprecated\",\"type\":\"boolean\"},{\"name\":\"deprecatedDate\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"dataClassification\",\"type\":{\"type\":\"enum\",\"name\":\"DataClassification\",\"namespace\":\"hydra.kafka.model\",\"symbols\":[\"Public\",\"InternalUseOnly\",\"ConfidentialPII\",\"RestrictedFinancial\",\"RestrictedEmployeeData\"]}},{\"name\":\"contact\",\"type\":{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"Email\",\"namespace\":\"hydra.kafka.model.ContactMethod\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"}]},{\"type\":\"record\",\"name\":\"Slack\",\"namespace\":\"hydra.kafka.model.ContactMethod\",\"fields\":[{\"name\":\"channel\",\"type\":\"string\"}]}]}},{\"name\":\"createdDate\",\"type\":\"string\"},{\"name\":\"parentSubjects\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"notes\",\"type\":[\"null\",\"string\"]},{\"name\":\"teamName\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"tags\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"string\"}],\"default\":null},{\"name\":\"notificationUrl\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
    def parser = new Schema.Parser()
    val schemaVersion1 = parser.parse(schemaVersion1String)
    val schemaVersion2 = parser.parse(schemaVersion2String)
    val schemaVersion3 = parser.parse(schemaVersion3String)
    val schemaVersion4 = parser.parse(schemaVersion4String)
    val schemaVersion5 = parser.parse(schemaVersion5String)
    val schemaCurrent = TopicMetadataV2ValueOptionalTagList.codec.schema.toOption.get
    val previousVersions = List(schemaVersion1, schemaVersion2, schemaVersion3, schemaVersion4, schemaVersion5)
    AvroCompatibilityChecker.FULL_TRANSITIVE_CHECKER.isCompatible(schemaCurrent, previousVersions.asJava) shouldBe true
  }
}
