package hydra.kafka.serializers

import java.time.Instant

import cats.data.NonEmptyList
import hydra.core.marshallers._
import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataContainer
import hydra.kafka.model.ContactMethod.{Email, Slack}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model._
import hydra.kafka.serializers.Errors._
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

class TopicMetadataV2ParserSpec extends AnyWordSpecLike with Matchers {
  import TopicMetadataV2Parser._
  import spray.json._

  val validAvroSchema =
    """
      |{
      |  "namespace": "_hydra.metadata",
      |  "name": "SomeName",
      |  "type": "record",
      |  "version": 1,
      |  "fields": [
      |    {
      |      "name": "id",
      |      "type": "string"
      |    }
      |  ]
      |}
      |""".stripMargin.parseJson

  val invalidAvroSchemaNamespace =
    """
      |{
      |  "namespace": "_hydra.meta-data",
      |  "name": "SomeName",
      |  "type": "record",
      |  "version": 1,
      |  "fields": [
      |    {
      |      "name": "id",
      |      "type": "string"
      |    }
      |  ]
      |}
      |""".stripMargin.parseJson

  val invalidAvroSchemaName =
    """
      |{
      |  "namespace": "_hydra.metadata",
      |  "name": "Some-Name",
      |  "type": "record",
      |  "version": 1,
      |  "fields": [
      |    {
      |      "name": "id",
      |      "type": "string"
      |    }
      |  ]
      |}
      |""".stripMargin.parseJson

  val invalidAvroSchemaNestedNamespace =
    """
      |{
      |  "namespace": "_hydra.metadata",
      |  "name": "SomeName",
      |  "type": "record",
      |  "version": 1,
      |  "fields": [
      |    {
      |      "name": "int_rec",
      |      "type": {
      |         "type" : "record",
      |         "name" : "int_record",
      |         "namespace": "nested-namespace",
      |         "fields" : [
      |           {
      |             "name": "rec_int",
      |             "type": "int"
      |           }
      |         ]
      |       }
      |    }
      |  ]
      |}
      |""".stripMargin.parseJson

  val invalidAvroSchemaNestedName =
    """
      |{
      |  "namespace": "_hydra.metadata",
      |  "name": "SomeName",
      |  "type": "record",
      |  "version": 1,
      |  "fields": [
      |    {
      |      "name": "int_rec",
      |      "type": {
      |         "type" : "record",
      |         "name" : "int-record",
      |         "namespace": "nested",
      |         "fields" : [
      |           {
      |             "name": "rec_int",
      |             "type": "int"
      |           }
      |         ]
      |       }
      |    }
      |  ]
      |}
      |""".stripMargin.parseJson

  "TopicMetadataV2Deserializer" must {

    "return instant.now" in {
      InstantFormat
        .read(JsNull)
        .toEpochMilli shouldBe (Instant.now.toEpochMilli +- 10.seconds.toMillis)
    }

    "parse list of contact method with email and slack channel" in {
      val email = "dataplatform@pluralsight.com"
      val slackChannel = "#dev-data-platform"
      val json =
        s"""
          |{
          | "email":"$email",
          | "slackChannel":"$slackChannel"
          |}
          |""".stripMargin
      val jsValue = json.parseJson
      ContactFormat.read(jsValue).toList should contain allOf (Slack
        .create(
          slackChannel
        )
        .get, Email.create(email).get)
    }

    "parse list of contact method with only slack channel" in {
      val slackChannel = "#dev-data-platform"
      val json =
        s"""
           |{
           | "slackChannel":"$slackChannel"
           |}
           |""".stripMargin
      val jsValue = json.parseJson
      val contactList = ContactFormat.read(jsValue)
      contactList.head shouldBe Slack.create(slackChannel).get
      contactList should have length 1
    }

    "parse list of contact method with only an email" in {
      val email = "dataplatform@pluralsight.com"
      val json =
        s"""
           |{
           | "email":"$email"
           |}
           |""".stripMargin
      val jsValue = json.parseJson
      val contactList = ContactFormat.read(jsValue)
      contactList.head shouldBe Email.create(email).get
      contactList should have length 1
    }

    "throw error when parsing list of contact method with no required fields" in {
      val jsValue = JsObject.empty
      the[DeserializationException] thrownBy {
        ContactFormat.read(jsValue)
      } should have message ContactMissingContactOption.errorMessage
    }

    "parse one of each type of StreamType" in {
      StreamTypeV2Format.read(JsString("Entity")) shouldBe StreamTypeV2.Entity
      StreamTypeV2Format.read(JsString("Event")) shouldBe StreamTypeV2.Event
      StreamTypeV2Format.read(JsString("Telemetry")) shouldBe StreamTypeV2.Telemetry
    }

    "throw error when parsing StreamType" in {
      val jsValue = JsString.empty
      import scala.reflect.runtime.{universe => ru}
      val tpe = ru.typeOf[StreamTypeV2]
      val knownDirectSubclasses: Set[ru.Symbol] =
        tpe.typeSymbol.asClass.knownDirectSubclasses

      the[DeserializationException] thrownBy {
        StreamTypeV2Format.read(jsValue)
      } should have message StreamTypeInvalid(jsValue, knownDirectSubclasses).errorMessage
    }

    "parse one of each type of DataClassification" in {
      DataClassificationFormat.read(JsString("Public")) shouldBe Public
      DataClassificationFormat.read(JsString("InternalUseOnly")) shouldBe InternalUseOnly
      DataClassificationFormat.read(JsString("ConfidentialPII")) shouldBe ConfidentialPII
      DataClassificationFormat.read(JsString("RestrictedFinancial")) shouldBe RestrictedFinancial
      DataClassificationFormat.read(JsString("RestrictedEmployeeData")) shouldBe RestrictedEmployeeData
    }

    "throw error when parsing DataClassification" in {
      val jsValue = JsString.empty
      import scala.reflect.runtime.{universe => ru}
      val tpe = ru.typeOf[DataClassification]
      val knownDirectSubclasses: Set[ru.Symbol] =
        tpe.typeSymbol.asClass.knownDirectSubclasses

      the[DeserializationException] thrownBy {
        DataClassificationFormat.read(jsValue)
      } should have message DataClassificationInvalid(
        jsValue,
        knownDirectSubclasses
      ).errorMessage
    }

    "parse a valid schema" in {
      val jsValue = validAvroSchema
      new SchemaFormat(isKey = false).read(jsValue).getName shouldBe "SomeName"
    }

    "throw an error given an invalid schema" in {
      val jsValue = JsObject.empty
      the[DeserializationException] thrownBy {
        new SchemaFormat(isKey = false).read(jsValue).getName
      } should have message InvalidSchema(jsValue, isKey = false).errorMessage
    }

    "throw an error with '-' in the namespace of a schema" in {
      the[DeserializationException] thrownBy {
        new SchemaFormat(isKey = false).read(invalidAvroSchemaNamespace).getName
      } should have message InvalidSchema(invalidAvroSchemaNamespace, isKey = false).errorMessage
    }

    "throw an error with '-' in the name of a schema" in {
      the[DeserializationException] thrownBy {
        new SchemaFormat(isKey = false).read(invalidAvroSchemaName).getName
      } should have message InvalidSchema(invalidAvroSchemaName, isKey = false).errorMessage
    }

    "throw an error with '-' in the namespace of a nested schema" in {
      the[DeserializationException] thrownBy {
        new SchemaFormat(isKey = false).read(invalidAvroSchemaNestedNamespace).getName
      } should have message InvalidSchema(invalidAvroSchemaNestedNamespace, isKey = false).errorMessage
    }

    "throw an error with '-' in the name of a nested schema" in {
      the[DeserializationException] thrownBy {
        new SchemaFormat(isKey = false).read(invalidAvroSchemaNestedName).getName
      } should have message InvalidSchema(invalidAvroSchemaNestedName, isKey = false).errorMessage
    }

    "parse a valid Schemas object" in {
      val json =
        s"""
          |{
          | "key":${validAvroSchema.compactPrint},
          |"value":${validAvroSchema.compactPrint}
          |}
          |""".stripMargin.parseJson
      SchemasFormat.read(json) shouldBe Schemas(
        new SchemaFormat(isKey = true).read(validAvroSchema),
        new SchemaFormat(isKey = false).read(validAvroSchema)
      )
    }

    "throw a comprehensive error given an incomplete Schemas object" in {
      val errorMessage = IncompleteSchemas(
        List(
          InvalidSchema(JsObject.empty, isKey = true).errorMessage,
          InvalidSchema(JsObject.empty, isKey = false).errorMessage
        ).mkString(" ")
      ).errorMessage
      the[DeserializationException] thrownBy {
        val json = JsObject.empty
        SchemasFormat.read(json)
      } should have message errorMessage
    }

    "throw a generic schemas error given a random non JsObject" in {
      the[DeserializationException] thrownBy {
        val json = JsString.empty
        SchemasFormat.read(json)
      } should have message InvalidSchemas(JsString.empty).errorMessage
    }

    "parse a complete object and return a TopicMetadataV2Request" in {
      val (
        jsonData,
        subject,
        streamType,
        deprecated,
        dataClassification,
        email,
        slackChannel,
        parentSubjects,
        notes,
        teamName
      ) =
        createJsValueOfTopicMetadataV2Request(
          Subject.createValidated("dvs.Foo").get,
          "#slack_channel",
          "email@address.com",
          "dvs-teamName"
        )()
      val tmv2 = TopicMetadataV2Format.read(jsonData)

      tmv2 shouldBe
        TopicMetadataV2Request(
          Schemas(
            new SchemaFormat(isKey = true).read(validAvroSchema),
            new SchemaFormat(isKey = false).read(validAvroSchema)
          ),
          streamType,
          deprecated,
          None,
          dataClassification,
          NonEmptyList(email, slackChannel :: Nil),
          tmv2.createdDate,
          parentSubjects,
          notes,
          Some(teamName)
        )
    }

    "parse a complete object with no optional fields and return a TopicMetadataV2Request" in {
      val (
        jsonData,
        subject,
        streamType,
        _,
        dataClassification,
        email,
        slackChannel,
        _,
        notes,
        teamName
      ) =
        createJsValueOfTopicMetadataV2Request(
          Subject.createValidated("dvs.Foo").get,
          "#slack_channel",
          "email@address.com",
          "dvs-teamName",
          allOptionalFieldsPresent = false
        )()
      val tmv2 = TopicMetadataV2Format.read(jsonData)

      tmv2 shouldBe
        TopicMetadataV2Request(
          Schemas(
            new SchemaFormat(isKey = true).read(validAvroSchema),
            new SchemaFormat(isKey = false).read(validAvroSchema)
          ),
          streamType,
          deprecated = false,
          None,
          dataClassification,
          NonEmptyList(email, slackChannel :: Nil),
          tmv2.createdDate,
          parentSubjects = List(),
          notes,
          Some(teamName)
        )
    }

    "throw deserialization error with invalid payload" in {
      the[DeserializationException] thrownBy {
        TopicMetadataV2Format.read(JsString.empty)
      } should have message invalidPayloadProvided(JsString.empty)
    }

    def containsAllOf(error: Throwable, errorMessages: String*) =
      errorMessages.forall(error.getMessage.contains)

    "accumulate the errors from an empty object payload" in {
      val error = the[DeserializationException] thrownBy {
        TopicMetadataV2Format.read(JsObject.empty)
      }
      assert(
        containsAllOf(
          error,
          "Field `schemas`",
          "Field `streamType`",
          "Field `dataClassification`",
          "Field `contact`"
        )
      )
    }

  }

  private def createJsValueOfTopicMetadataV2Request(
      subject: Subject,
      slackChannel: String,
      email: String,
      teamName: String,
        allOptionalFieldsPresent: Boolean = true
  )(
      streamType: StreamTypeV2 = StreamTypeV2.Entity,
      deprecated: Boolean = false,
      dataClassification: DataClassification = Public,
      validAvroSchema: JsValue = validAvroSchema,
      parentSubjects: List[Subject] = List(),
      notes: Option[String] = None,
      createdDate: Instant = Instant.now(),
  ): (
      JsValue,
      Subject,
      StreamTypeV2,
      Boolean,
      DataClassification,
      Email,
      Slack,
      List[Subject],
      Option[String],
      String
  ) = {
    val jsValue = s"""
         |{
         |  "schemas": {
         |   "key": ${validAvroSchema.compactPrint},
         |   "value": ${validAvroSchema.compactPrint}
         |  },
         |  "streamType": "${streamType.toString}",
         |  "dataClassification":"${dataClassification.toString}",
         |  "teamName":"${teamName}",
         |  "contact": {
         |    "slackChannel": "$slackChannel",
         |    "email": "$email"
         |  }
         |  ${if (allOptionalFieldsPresent) {
                       s""","parentSubjects": ${parentSubjects.toJson.compactPrint},"deprecated":$deprecated,"createdDate":"${createdDate.toString}""""
                     } else ""}
         |  ${if (notes.isDefined) s""","notes": "${notes.get}"""" else ""}}
         |""".stripMargin.parseJson
    (
      jsValue,
      subject,
      streamType,
      deprecated,
      dataClassification,
      Email.create(email).get,
      Slack.create(slackChannel).get,
      parentSubjects,
      notes,
      teamName
    )
  }

  "TopicMetadataV2Serializer" must {

    "serialize a subject as a string" in {
      val subjectName = "dvs.ValidSubjectName"
      val subject = Subject.createValidated(subjectName).get
      SubjectFormat.write(subject) shouldBe JsString(subjectName)
    }

    "serialize an instant" in {
      val dateString = "2020-02-02T12:34:56Z"
      val instant = Instant.parse(dateString)
      InstantFormat.write(instant) shouldBe JsString(dateString)
    }

    "serialize a list of contactMethod objects" in {
      val email = Email.create("some@address.com").get
      val slack = Slack.create("#this_is_my_slack_channel").get

      ContactFormat.write(NonEmptyList(email, Nil)) shouldBe JsObject(
        Map("email" -> JsString(email.address.value))
      )
      ContactFormat.write(NonEmptyList(slack, Nil)) shouldBe JsObject(
        Map("slackChannel" -> JsString(slack.channel.value))
      )
      val jObject = JsObject(
        Map(
          "email" -> JsString(email.address.value),
          "slackChannel" -> JsString(slack.channel.value)
        )
      )
      ContactFormat.write(NonEmptyList(email, slack :: Nil)) shouldBe jObject
      jObject.compactPrint shouldBe s"""{"email":"${email.address}","slackChannel":"${slack.channel}"}"""
    }

    "serialize a StreamType" in {
      val streamType = StreamTypeV2.Entity
      StreamTypeV2Format.write(streamType) shouldBe JsString("Entity")
    }

    "serialize a DataClassificationFormat" in {
      DataClassificationFormat.write(Public) shouldBe JsString("Public")
    }

    "serialize an avro schema" in {
      val schema = new SchemaFormat(isKey = true).read(validAvroSchema)
      new SchemaFormat(isKey = true)
        .write(schema)
        .compactPrint shouldBe validAvroSchema.compactPrint
    }

    "serialize the Schemas object" in {
      val keySchema = new SchemaFormat(isKey = true).read(validAvroSchema)
      val valueSchema = new SchemaFormat(isKey = false).read(validAvroSchema)
      SchemasFormat.write(Schemas(keySchema, valueSchema)) shouldBe JsObject(
        Map(
          "key" -> new SchemaFormat(true).write(keySchema),
          "value" -> new SchemaFormat(false).write(valueSchema)
        )
      )
    }

    "serialize the entire topicMetadata Request payload" in {
      val subject = Subject.createValidated("dvs.some-valid-subject-name").get
      val keySchema = new SchemaFormat(isKey = true).read(validAvroSchema)
      val valueSchema = new SchemaFormat(isKey = false).read(validAvroSchema)
      val streamType = StreamTypeV2.Entity
      val deprecated = false
      val deprecatedDate = None
      val dataClassification = Public
      val email = Email.create("some@address.com").get
      val slack = Slack.create("#valid_slack_channel").get
      val contact = NonEmptyList(email, slack :: Nil)
      val createdDate = Instant.now
      val parentSubjects = List(
        Subject.createValidated("dvs.valid-parent-1").get,
        Subject.createValidated("dvs.valid-parent-2").get
      )
      val notes = Some("Notes go here.")
      val teamName = "dvs-teamName"

      val topicMetadataV2 = TopicMetadataV2Request(
        schemas = Schemas(
          keySchema,
          valueSchema
        ),
        streamType = streamType,
        deprecated = deprecated,
        deprecatedDate,
        dataClassification = dataClassification,
        contact = contact,
        createdDate = createdDate,
        parentSubjects = parentSubjects,
        notes = notes,
        teamName = Some(teamName)
      )
      TopicMetadataV2Format.write(topicMetadataV2) shouldBe
        createJsValueOfTopicMetadataV2Request(
          subject,
          slack.channel.value,
          email.address.value,
          teamName
        )(
          streamType,
          deprecated,
          dataClassification,
          validAvroSchema,
          parentSubjects,
          notes,
          createdDate
        )._1
    }

  }

  "TopicMetadataV2Parser" must {

    "TopicMetadataV2Format write matches TopicMetadataResponseV2Format write" in {
      val subject = Subject.createValidated("dvs.valid").get
      val tmc = TopicMetadataContainer(TopicMetadataV2Key(subject),
        TopicMetadataV2Value(StreamTypeV2.Entity, false, None, Public,
          NonEmptyList.one(ContactMethod.create("blah@pluralsight.com").get),
          Instant.now(), List.empty, None, Some("dvs-teamName")),
        Some(new SchemaFormat(isKey = true).read(validAvroSchema)),
        Some(new SchemaFormat(isKey = false).read(validAvroSchema)))
      val response = TopicMetadataV2Response.fromTopicMetadataContainer(tmc)
      val request = TopicMetadataV2Request.apply(Schemas(tmc.keySchema.get, tmc.valueSchema.get),tmc.value.streamType,
        tmc.value.deprecated,tmc.value.deprecatedDate,tmc.value.dataClassification,tmc.value.contact,
        tmc.value.createdDate,tmc.value.parentSubjects,tmc.value.notes, teamName = tmc.value.teamName)

      TopicMetadataV2Format.write(request).compactPrint shouldBe
        TopicMetadataResponseV2Format.write(response).compactPrint.replace(",\"subject\":\"dvs.valid\"", "")
    }

    def createSchema: Schema = {
      SchemaBuilder
        .record("mySchema")
        .fields()
        .name("isTrue")
        .`type`()
        .stringType()
        .noDefault()
        .endRecord()
    }

    "write maybeSchemas" in {
      val maybeSchemas = MaybeSchemas(None, Some(createSchema))
      MaybeSchemasFormat.write(maybeSchemas) shouldBe JsObject(
        Map(
          "key" -> JsString("Unable to retrieve Key Schema"),
          "value" -> new SchemaFormat(isKey = false).write(createSchema)
        )
      )

      val missingVal = MaybeSchemas(Some(createSchema), None)
      MaybeSchemasFormat.write(missingVal) shouldBe JsObject(
        Map(
          "value" -> JsString("Unable to retrieve Value Schema"),
          "key" -> new SchemaFormat(isKey = true).write(createSchema)
        )
      )
    }

    "make sure deprecatedDate works with deprecated true None for Deprecated Date" in {
      val subject = Subject.createValidated("dvs.valid").get
      val before = Instant.now
      val tmc = TopicMetadataContainer(TopicMetadataV2Key(subject),
        TopicMetadataV2Value(StreamTypeV2.Entity, true, None,
          Public, NonEmptyList.one(ContactMethod.create("blah@pluralsight.com").get), Instant.now(), List.empty, None, Some("dvs-teamName")),
        Some(new SchemaFormat(isKey = true).read(validAvroSchema)),
        Some(new SchemaFormat(isKey = false).read(validAvroSchema)))
      val request = TopicMetadataV2Request.apply(Schemas(tmc.keySchema.get, tmc.valueSchema.get),tmc.value.streamType,
        tmc.value.deprecated,tmc.value.deprecatedDate,tmc.value.dataClassification,tmc.value.contact,
        tmc.value.createdDate,tmc.value.parentSubjects,tmc.value.notes,tmc.value.teamName)
      val firstDeprecatedDate = TopicMetadataV2Format.read(request.toJson).deprecatedDate.getOrElse(None)
      firstDeprecatedDate shouldBe None
    }

    "make sure deprecatedDate works with deprecated true Instant for Deprecated Date" in {
      val subject = Subject.createValidated("dvs.valid").get
      val now = Instant.now
      val tmc = TopicMetadataContainer(TopicMetadataV2Key(subject),
        TopicMetadataV2Value(StreamTypeV2.Entity, true, Some(now),
          Public, NonEmptyList.one(ContactMethod.create("blah@pluralsight.com").get), Instant.now(), List.empty, None, Some("dvs-teamName")),
        Some(new SchemaFormat(isKey = true).read(validAvroSchema)),
        Some(new SchemaFormat(isKey = false).read(validAvroSchema)))
      val request = TopicMetadataV2Request.apply(Schemas(tmc.keySchema.get, tmc.valueSchema.get),tmc.value.streamType,
        tmc.value.deprecated,tmc.value.deprecatedDate,tmc.value.dataClassification,tmc.value.contact,tmc.value.createdDate,
        tmc.value.parentSubjects,tmc.value.notes,tmc.value.teamName)
      val firstDeprecatedDate = TopicMetadataV2Format.read(request.toJson).deprecatedDate.get
      val now2 = Instant.now
      now2.isAfter(firstDeprecatedDate) shouldBe true
      now shouldBe firstDeprecatedDate
    }

    "make sure deprecatedDate works with deprecated false" in {
      val subject = Subject.createValidated("dvs.valid").get
      val tmc = TopicMetadataContainer(TopicMetadataV2Key(subject),
        TopicMetadataV2Value(StreamTypeV2.Entity, false, None,
          Public, NonEmptyList.one(ContactMethod.create("blah@pluralsight.com").get), Instant.now(), List.empty, None, Some("dvs-teamName")),
        Some(new SchemaFormat(isKey = true).read(validAvroSchema)),
        Some(new SchemaFormat(isKey = false).read(validAvroSchema)))
      val request = TopicMetadataV2Request.apply(Schemas(tmc.keySchema.get, tmc.valueSchema.get),tmc.value.streamType,
        tmc.value.deprecated,tmc.value.deprecatedDate,tmc.value.dataClassification,tmc.value.contact,
        tmc.value.createdDate,tmc.value.parentSubjects,tmc.value.notes, tmc.value.teamName)
      val firstDeprecatedDate = TopicMetadataV2Format.read(request.toJson).deprecatedDate.getOrElse(None)
      firstDeprecatedDate shouldBe None
    }

  }
}
