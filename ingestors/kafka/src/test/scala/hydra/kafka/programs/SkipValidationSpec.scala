package hydra.kafka.programs

import cats.effect._
import cats.syntax.all._
import hydra.common.validation.ValidationError.ValidationCombinedErrors
import hydra.kafka.IOSuite
import hydra.kafka.model.SkipValidation
import hydra.kafka.programs.TopicSchemaError._
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class SkipValidationSpec extends AsyncFreeSpec with IOSuite {
  import CreateTopicProgramSpec._

  "SkipValidationSpec" - {

    "skip validation: empty key schema fields" in {
      val emptyFieldsSchema = SchemaBuilder.record("EmptyFieldsSchema").fields().endRecord()

      val result = createTopicMetadataV2Request(emptyFieldsSchema)
      result.attempt.map(_ shouldBe KeyIsEmptyError.asLeft)

      val resultWithSkipValidation = createTopicMetadataV2Request(emptyFieldsSchema, skipValidations = Some(List(SkipValidation.emptyKeyFields)))
      resultWithSkipValidation.map(_ shouldBe ())
    }

    "skip validation: key schema evolution" in {
      val schemaString =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "keyThing",
          |      "doc": "text",
          |      "type":{
          |        "type": "string",
          |        "logicalType":"uuid"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin
      val evolvedSchemaString =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "keyThing",
          |      "doc": "text",
          |      "type":{
          |        "type": "string",
          |        "logicalType":"date"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin

      val schema = new Schema.Parser().parse(schemaString)
      val evolvedSchema = new Schema.Parser().parse(evolvedSchemaString)

      val result = evolveTopicMetadataV2Request(keySchema = schema, evolvedKeySchema = evolvedSchema)
      result.attempt.map(_ shouldBe IllegalLogicalTypeChangeError("uuid", "date", "keyThing").asLeft)

      val resultWithSkipValidation = evolveTopicMetadataV2Request(keySchema = schema, evolvedKeySchema = evolvedSchema,
        skipValidations = Some(List(SkipValidation.keySchemaEvolution)))
      resultWithSkipValidation.attempt.map(_ shouldBe ().asRight)
    }

    "skip validation: value schema evolution" in {
      val schemaString =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "id",
          |       "default": "abc",
          |       "type": ["string", "null"],
          |       "doc": "text"
          |     },
          |    {
          |      "name": "createdAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      },
          |      "doc": "text"
          |    },
          |    {
          |      "name": "updatedAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      },
          |      "doc": "text"
          |    }
          |  ]
          |}
        """.stripMargin
      val evolvedSchemaString =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |        "name":"id",
          |        "default": "abc",
          |        "doc": "text",
          |        "type":[
          |                 {
          |                   "type": "string",
          |                   "logicalType": "uuid"
          |                 },
          |                 "null"
          |               ]
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
        """.stripMargin

      val schema = new Schema.Parser().parse(schemaString)
      val evolvedSchema = new Schema.Parser().parse(evolvedSchemaString)

      val result = evolveTopicMetadataV2Request(valueSchema = schema, evolvedValueSchema = evolvedSchema)
      result.attempt.map(_ shouldBe IllegalLogicalTypeChangeError("null", "uuid", "id").asLeft)

      val resultWithSkipValidation = evolveTopicMetadataV2Request(valueSchema = schema, evolvedValueSchema = evolvedSchema,
        skipValidations = Some(List(SkipValidation.valueSchemaEvolution)))
      resultWithSkipValidation.attempt.map(_ shouldBe ().asRight)
    }

    "skip validation: required doc field in key schema" in {
      val schemaWithoutDocField =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "keyThing",
          |      "type":{
          |        "type": "string",
          |        "logicalType":"uuid"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin

      val schema = new Schema.Parser().parse(schemaWithoutDocField)

      val result = createTopicMetadataV2Request(keySchema = schema)
      result.attempt.map(_ shouldBe RequiredSchemaKeyFieldMissingError("doc", schema, "Entity").asLeft)

      val resultWithSkipValidation = createTopicMetadataV2Request(keySchema = schema, skipValidations = Some(List(SkipValidation.requiredDocField)))
      resultWithSkipValidation.attempt.map(_ shouldBe ().asRight)
    }

    "skip validation: required doc field in value schema" in {
      val schemaWithoutDocField =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |    {
          |      "name": "createdAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      },
          |      "doc": "text"
          |    }
          |  ]
          |}
        """.stripMargin

      val schema = new Schema.Parser().parse(schemaWithoutDocField)

      val result = createTopicMetadataV2Request(valueSchema = schema)
      result.attempt.map(_ shouldBe RequiredSchemaValueFieldMissingError("doc", schema, "Entity").asLeft)

      val resultWithSkipValidation = createTopicMetadataV2Request(valueSchema = schema, skipValidations = Some(List(SkipValidation.requiredDocField)))
      resultWithSkipValidation.attempt.map(_ shouldBe ().asRight)
    }

    "skip validation: required createdAt field in value schema" in {
      val schemaWithoutDocField =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |    {
          |      "name": "updatedAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      },
          |      "doc": "text"
          |    }
          |  ]
          |}
        """.stripMargin

      val schema = new Schema.Parser().parse(schemaWithoutDocField)

      val result = createTopicMetadataV2Request(valueSchema = schema)
      result.attempt.map(_ shouldBe RequiredSchemaValueFieldMissingError("createdAt", schema, "Entity").asLeft)

      val resultWithSkipValidation = createTopicMetadataV2Request(valueSchema = schema, skipValidations = Some(List(SkipValidation.requiredCreatedAtField)))
      resultWithSkipValidation.attempt.map(_ shouldBe ().asRight)
    }

    "skip validation: required updatedAt field in value schema" in {
      val schemaWithoutDocField =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |    {
          |      "name": "createdAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      },
          |      "doc": "text"
          |    }
          |  ]
          |}
        """.stripMargin

      val schema = new Schema.Parser().parse(schemaWithoutDocField)

      val result = createTopicMetadataV2Request(valueSchema = schema)
      result.attempt.map(_ shouldBe RequiredSchemaValueFieldMissingError("updatedAt", schema, "Entity").asLeft)

      val resultWithSkipValidation = createTopicMetadataV2Request(valueSchema = schema, skipValidations = Some(List(SkipValidation.requiredUpdatedAtField)))
      resultWithSkipValidation.attempt.map(_ shouldBe ().asRight)
    }

    "skip validation: same field type mismatch in key and value schemas" in {
      val keySchemaString =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "keyThing",
          |      "doc": "text",
          |      "type":{
          |        "type": "string",
          |        "logicalType":"uuid"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin
      val valueSchemaString =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |        "name":"keyThing",
          |        "doc": "text",
          |        "type": "string"
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
        """.stripMargin
      val keySchema = new Schema.Parser().parse(keySchemaString)
      val valueSchema = new Schema.Parser().parse(valueSchemaString)
      val keyFieldSchema = keySchema.getField("keyThing").schema()
      val valueFieldSchema = valueSchema.getField("keyThing").schema()

      val result = createTopicMetadataV2Request(keySchema, valueSchema)
      result.attempt.map(_ shouldBe IncompatibleKeyAndValueFieldNamesError("keyThing", keyFieldSchema, valueFieldSchema).asLeft)

      val resultWithSkipValidation = createTopicMetadataV2Request(keySchema, valueSchema,
        skipValidations = Some(List(SkipValidation.sameFieldsTypeMismatchInKeyValueSchemas)))
      resultWithSkipValidation.attempt.map(_ shouldBe ().asRight)
    }

    "skip validation: nullable key schema fields" in {
      val keySchemaString =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |       "name": "nullableField",
          |       "type": "null",
          |       "doc": "text"
          |     }
          |  ]
          |}
        """.stripMargin

      val keySchema = new Schema.Parser().parse(keySchemaString)
      val fieldSchema = keySchema.getField("nullableField").schema()

      val result = createTopicMetadataV2Request(keySchema = keySchema)
      result.attempt.map(_ shouldBe KeyHasNullableFieldError("nullableField", fieldSchema).asLeft)

      val resultWithSkipValidation = createTopicMetadataV2Request(keySchema = keySchema,
        skipValidations = Some(List(SkipValidation.nullableFieldsInKeySchema)))
      resultWithSkipValidation.attempt.map(_ shouldBe ().asRight)
    }

    "skip validation: missing default field in nullable field of value schema" in {
      val valueSchemaString =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |     {
          |        "name":"nullableField",
          |        "doc": "text",
          |        "type": ["null", "string"]
          |     },
          |    {
          |      "name": "createdAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    },
          |    {
          |      "name": "updatedAt",
          |      "doc": "text",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
        """.stripMargin
      val valueSchema = new Schema.Parser().parse(valueSchemaString)
      val valueFieldSchema = valueSchema.getField("nullableField").schema()

      val result = createTopicMetadataV2Request(valueSchema=valueSchema)
      result.attempt.map(_ shouldBe NullableFieldWithoutDefaultValueError("nullableField", valueFieldSchema).asLeft)

      val resultWithSkipValidation = createTopicMetadataV2Request(valueSchema=valueSchema,
        skipValidations = Some(List(SkipValidation.missingDefaultInNullableFieldsOfValueSchema)))
      resultWithSkipValidation.attempt.map(_ shouldBe ().asRight)
    }

    "skip validation: unsupported logical type field in key schema" in {
      val keySchemaString =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "timestamp",
          |      "doc": "text",
          |      "type":{
          |        "type": "string",
          |        "logicalType": "iso-datetime"
          |      }
          |    }
          |  ]
          |}
      """.stripMargin

      val keySchema = new Schema.Parser().parse(keySchemaString)

      val result = createTopicMetadataV2Request(keySchema = keySchema)
      result.attempt.map(_ shouldBe UnsupportedLogicalType(keySchema.getField("timestamp"), "iso-datetime").asLeft)

      val resultWithSkipValidation = createTopicMetadataV2Request(keySchema = keySchema,
        skipValidations = Some(List(SkipValidation.unsupportedLogicalTypeFieldsInKeySchema)))
      resultWithSkipValidation.attempt.map(_ shouldBe ().asRight)
    }

    "skip validation: unsupported logical type field in value schema" in {
      val valueSchemaString =
        """
          |{
          |  "type": "record",
          |  "name": "Date",
          |  "fields": [
          |    {
          |      "name": "timestamp",
          |      "type":{
          |        "type": "string",
          |        "logicalType": "iso-datetime"
          |      },
          |      "doc": "text"
          |    },
          |    {
          |      "name": "createdAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      },
          |      "doc": "text"
          |    },
          |    {
          |      "name": "updatedAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      },
          |      "doc": "text"
          |    }
          |  ]
          |}
      """.stripMargin
      val valueSchema = new Schema.Parser().parse(valueSchemaString)

      val result = createTopicMetadataV2Request(valueSchema = valueSchema)
      result.attempt.map(_ shouldBe UnsupportedLogicalType(valueSchema.getField("timestamp"), "iso-datetime").asLeft)

      val resultWithSkipValidation = createTopicMetadataV2Request(valueSchema = valueSchema,
        skipValidations = Some(List(SkipValidation.unsupportedLogicalTypeFieldsInValueSchema)))
      resultWithSkipValidation.attempt.map(_ shouldBe ().asRight)
    }

    "skip validation: default loophole in a required field" in {
      val schemaWithDefaultLoophole =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |    {
          |      "name": "createdAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      },
          |      "doc": "text",
          |      "default": 0
          |    },
          |    {
          |      "name": "updatedAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      },
          |      "doc": "text"
          |    }
          |  ]
          |}
        """.stripMargin

      val schema = new Schema.Parser().parse(schemaWithDefaultLoophole)

      val result = createTopicMetadataV2Request(valueSchema = schema)
      result.attempt.map(_ shouldBe RequiredSchemaValueFieldWithDefaultValueError("createdAt", schema, "Entity").asLeft)

      val resultWithSkipValidation = createTopicMetadataV2Request(valueSchema = schema,
        skipValidations = Some(List(SkipValidation.defaultLoopholeInRequiredField)))
      resultWithSkipValidation.attempt.map(_ shouldBe ().asRight)
    }

    "skip multiple validations: all" in {
      val valueSchemaString =
        """
          |{
          |  "type": "record",
          |  "name": "test",
          |  "fields": [
          |    {
          |      "name": "updatedAt",
          |      "type":{
          |        "type": "long",
          |        "logicalType":"timestamp-millis"
          |      }
          |    }
          |  ]
          |}
        """.stripMargin

      val valueSchema = new Schema.Parser().parse(valueSchemaString)

      val result = createTopicMetadataV2Request(valueSchema = valueSchema)
      result.attempt.map(_ shouldBe
        ValidationCombinedErrors(List(
          RequiredSchemaValueFieldMissingError("doc", valueSchema, "Entity").message,
          RequiredSchemaValueFieldMissingError("createdAt", valueSchema, "Entity").message
        )).asLeft)

      val resultWithSkipValidation = createTopicMetadataV2Request(valueSchema = valueSchema,
        skipValidations = Some(List(SkipValidation.requiredDocField, SkipValidation.requiredCreatedAtField)))
      resultWithSkipValidation.attempt.map(_ shouldBe ().asRight)

      val resultWithSkipValidationAll = createTopicMetadataV2Request(valueSchema = valueSchema, skipValidations = Some(List(SkipValidation.all)))
      resultWithSkipValidationAll.attempt.map(_ shouldBe ().asRight)
    }
  }

  private def createTopicMetadataV2Request(keySchema: Schema = keySchema, valueSchema: Schema = valueSchema,
                                           skipValidations: Option[List[SkipValidation]] = None) =
    for {
      ts <- Resource.eval(initTestServices())
      _ <- ts.program.registerSchemas(subject, keySchema, valueSchema)
      _ <- ts.program.createTopicResource(subject, topicDetails)
      _ <- Resource.eval(ts.program.createTopicFromMetadataOnly(
        subject,
        createTopicMetadataRequest(keySchema, valueSchema),
        withRequiredFields = true,
        skipValidations))
    } yield ()

  private def evolveTopicMetadataV2Request(keySchema: Schema = keySchema, valueSchema: Schema = valueSchema,
                                           evolvedKeySchema: Schema = keySchema, evolvedValueSchema: Schema = valueSchema,
                                           skipValidations: Option[List[SkipValidation]] = None) =
    for {
      ts <- Resource.eval(initTestServices())
      _ <- ts.program.registerSchemas(subject, keySchema, valueSchema)
      _ <- ts.program.createTopicResource(subject, topicDetails)
      _ <- Resource.eval(ts.program.createTopicFromMetadataOnly(
        subject,
        createTopicMetadataRequest(keySchema, valueSchema),
        withRequiredFields = true,
        skipValidations))
      _ <- Resource.eval(ts.program.createTopicFromMetadataOnly(
        subject,
        createTopicMetadataRequest(evolvedKeySchema, evolvedValueSchema),
        withRequiredFields = true,
        skipValidations))
    } yield ()
}
