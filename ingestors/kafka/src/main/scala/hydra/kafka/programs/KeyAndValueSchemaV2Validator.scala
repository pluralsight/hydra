package hydra.kafka.programs

import cats.data.{NonEmptyChain, Validated}
import cats.effect.Sync
import cats.syntax.all._
import hydra.avro.convert.IsoDate
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.model.{RequiredField, Schemas, StreamTypeV2, TopicMetadataV2Request}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.programs.TopicSchemaError._
import hydra.kafka.programs.Validator.ValidationChain
import org.apache.avro.{Schema, SchemaBuilder}
import RequiredFieldStructures._
import hydra.kafka.util.GenericUtils.postCutOffDate

import java.time.Instant
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

class KeyAndValueSchemaV2Validator[F[_]: Sync] private (schemaRegistry: SchemaRegistry[F]) extends Validator {
  def validate(request: TopicMetadataV2Request, subject: Subject, withRequiredFields: Boolean, createdDate: Option[Instant] = None): F[Unit] = {
    val schemas = request.schemas

    (schemas.key.getType, schemas.value.getType) match {
      case (Schema.Type.RECORD, Schema.Type.RECORD) => validateRecordRecordTypeSchemas(schemas, subject, request.streamType, withRequiredFields, createdDate)
      case (Schema.Type.STRING, Schema.Type.RECORD) if request.tags.contains("KSQL") => validateKSQLSchemas(schemas, subject, request.streamType)
      case _                                        => resultOf(Validated.Invalid(NonEmptyChain.one(InvalidSchemaTypeError)))
    }
  }

  private def validateKSQLSchemas(schemas: Schemas, subject: Subject, streamType: StreamTypeV2): F[Unit] = {
    val concoctedKeyFields = SchemaBuilder
      .record("uselessRecord") //This is a useless record whose only purpose is to transform the string key into a list of fields.
      .fields()
      .name(schemas.key.getName)
      .`type`()
      .stringType()
      .noDefault()
      .endRecord().getFields.asScala.toList

    val valueFields = schemas.value.getFields.asScala.toList
    val validators = for {
        keySchemaEvolutionValidationResult         <- validateKeySchemaEvolution(schemas, subject)
        valueSchemaEvolutionValidationResult       <- validateValueSchemaEvolution(schemas, subject)
        defaultNullableValueFieldsValidationResult <- checkForDefaultNullableValueFields(valueFields, streamType)
        unsupportedLogicalTypesKey                 <- checkForUnsupportedLogicalType(concoctedKeyFields)
        unsupportedLogicalTypesValues              <- checkForUnsupportedLogicalType(valueFields)
        mismatchesValidationResult                 <- checkForMismatches(concoctedKeyFields, valueFields)
      } yield {
        keySchemaEvolutionValidationResult ++
          valueSchemaEvolutionValidationResult ++
          defaultNullableValueFieldsValidationResult ++
          unsupportedLogicalTypesKey ++
          unsupportedLogicalTypesValues ++
          mismatchesValidationResult
      }

    resultOf(validators)
  }

  private def validateRecordRecordTypeSchemas(schemas: Schemas, subject: Subject, streamType: StreamTypeV2, withRequiredFields: Boolean,
                                              createdDate: Option[Instant]): F[Unit] = {
    val keyFields   = schemas.key.getFields.asScala.toList
    val valueFields = schemas.value.getFields.asScala.toList

    val validators = for {
      keyFieldsValidationResult                  <- validateKeyFields(keyFields)
      keySchemaEvolutionValidationResult         <- validateKeySchemaEvolution(schemas, subject)
      valueSchemaEvolutionValidationResult       <- validateValueSchemaEvolution(schemas, subject)
      validateRequiredKeyFieldsResult            <- if(withRequiredFields) validateRequiredKeyFields(schemas.key, streamType) else Nil.pure
      validateRequiredValueFieldsResult          <- if(withRequiredFields) validateRequiredValueFields(schemas.value, streamType, createdDate) else Nil.pure
      mismatchesValidationResult                 <- checkForMismatches(keyFields, valueFields)
      nullableKeyFieldsValidationResult          <- checkForNullableKeyFields(keyFields, streamType)
      defaultNullableValueFieldsValidationResult <- checkForDefaultNullableValueFields(valueFields, streamType)
      unsupportedLogicalTypesKey                 <- checkForUnsupportedLogicalType(keyFields)
      unsupportedLogicalTypesValues              <- checkForUnsupportedLogicalType(valueFields)
    } yield {
        (keyFieldsValidationResult +: keySchemaEvolutionValidationResult) ++
        valueSchemaEvolutionValidationResult ++
        mismatchesValidationResult ++
        nullableKeyFieldsValidationResult ++
        defaultNullableValueFieldsValidationResult ++
        unsupportedLogicalTypesKey ++
        unsupportedLogicalTypesValues ++
        validateRequiredKeyFieldsResult ++
        validateRequiredValueFieldsResult
    }

    resultOf(validators)
  }

  private def checkForUnsupportedLogicalType(fields: List[Schema.Field]): F[List[ValidationChain]] =
    fields.map { field =>
      validate(!getLogicalType(field.schema()).contains(IsoDate.IsoDateLogicalTypeName), UnsupportedLogicalType(field, getLogicalType(field.schema()).getOrElse("")))
    }.pure

  private def validateKeyFields(keyFields: List[Schema.Field]): F[ValidationChain] =
    validate(keyFields.nonEmpty, TopicSchemaError.KeyIsEmptyError).pure

  private def validateKeySchemaEvolution(schemas: Schemas, subject: Subject): F[List[ValidationChain]] =
    validateSchemaEvolution(schemas, subject + "-key")

  private def validateValueSchemaEvolution(schemas: Schemas, subject: Subject): F[List[ValidationChain]] =
    validateSchemaEvolution(schemas, subject + "-value")

  private def validateSchemaEvolution(schemas: Schemas, subject: String): F[List[ValidationChain]] =
    schemaRegistry.getLatestSchemaBySubject(subject).map {
      case Some(latestSchema) =>
        val schema = if (subject.endsWith("-key")) schemas.key else schemas.value
        checkForIllegalLogicalTypeEvolutions(latestSchema, schema, schema.getName)
      case _ => List(Validator.valid)
    }

  private def validateRequiredKeyFields(keySchema: Schema, streamType: StreamTypeV2): F[List[ValidationChain]] =
    validateRequiredFields(isKey = true, keySchema, streamType)

  private def validateRequiredValueFields(valueSchema: Schema, streamType: StreamTypeV2, createdDate: Option[Instant]): F[List[ValidationChain]] =
    validateRequiredFields(isKey = false, valueSchema, streamType, createdDate)

  private def validateRequiredFields(isKey: Boolean, schema: Schema, streamType: StreamTypeV2, createdDate: Option[Instant] = None): F[List[ValidationChain]] =
    streamType match {
      case (StreamTypeV2.Entity | StreamTypeV2.Event) =>
        if (isKey) {
          List(validate(docFieldValidator(schema), getFieldMissingError(isKey, RequiredField.DOC, schema, streamType.toString))).pure
        } else {
          val isCreatedAfterCutOffDate = postCutOffDate(createdDate, cutOffDate = sys.env.getOrElse("DEFAULT_LOOPHOLE_CUTOFF_DATE_IN_YYYYMMDD", "20230602"))
          List(
            validate(docFieldValidator(schema), getFieldMissingError(isKey, RequiredField.DOC, schema, streamType.toString)),
            validate(createdAtFieldValidator(schema), getFieldMissingError(isKey, RequiredField.CREATED_AT, schema, streamType.toString)),
            validate(updatedAtFieldValidator(schema), getFieldMissingError(isKey, RequiredField.UPDATED_AT, schema, streamType.toString)),
            validate(defaultFieldOfRequiredFieldValidator(schema, RequiredField.CREATED_AT, isCreatedAfterCutOffDate),
              RequiredSchemaValueFieldWithDefaultValueError(RequiredField.CREATED_AT, schema, streamType.toString)),
            validate(defaultFieldOfRequiredFieldValidator(schema, RequiredField.UPDATED_AT, isCreatedAfterCutOffDate),
              RequiredSchemaValueFieldWithDefaultValueError(RequiredField.UPDATED_AT, schema, streamType.toString))
          ).pure
        }
      case _ =>
        List(validate(docFieldValidator(schema), getFieldMissingError(isKey, RequiredField.DOC, schema, streamType.toString))).pure
    }

  private def checkForIllegalLogicalTypeEvolutions(existingSchema: Schema, newSchema: Schema, fieldName: String): List[ValidationChain] = {
    def isValidType(existingSchema: Schema, newSchema: Schema): Boolean =
      (existingSchema.getType, newSchema.getType) match {
        case (Schema.Type.ENUM | Schema.Type.FIXED, _)                      => true
        case _ if existingSchema.getLogicalType != newSchema.getLogicalType => false
        case _                                                              => true
      }

    (existingSchema.getType, newSchema.getType) match {
      case (Schema.Type.RECORD, Schema.Type.RECORD) =>
        existingSchema.getFields.asScala.toList.flatMap { existingField =>
          newSchema.getFields.asScala.toList.filter(f => f.name() == existingField.name()).flatMap { newField =>
            checkForIllegalLogicalTypeEvolutions(existingField.schema(), newField.schema(), existingField.name())
          }
        }
      case (Schema.Type.ARRAY, Schema.Type.ARRAY) =>
        checkForIllegalLogicalTypeEvolutions(existingSchema.getElementType, newSchema.getElementType, fieldName)
      case (Schema.Type.MAP, Schema.Type.MAP) =>
        checkForIllegalLogicalTypeEvolutions(existingSchema.getValueType, newSchema.getValueType, fieldName)
      case _ if existingSchema.isUnion && newSchema.isUnion =>
        existingSchema.getTypes.asScala.toList zip newSchema.getTypes.asScala.toList flatMap { t =>
          checkForIllegalLogicalTypeEvolutions(t._1, t._2, fieldName)
        }
      case _ => List(validate(isValidType(existingSchema, newSchema),
        IllegalLogicalTypeChangeError(getLogicalType(existingSchema).getOrElse("null"), getLogicalType(newSchema).getOrElse("null"), fieldName)))
    }
  }

  private def getLogicalType(schema: Schema): Option[String] =
    Option(schema.getLogicalType)
      .fold(Option(schema.getProp("logicalType")))(_.getName.some)

  private def checkForNullableKeyFields(keyFields: List[Schema.Field], streamType: StreamTypeV2): F[List[ValidationChain]] = {
    def fieldIsNotNull(field: Schema.Field): Boolean =
      field.schema().getType match {
        case Schema.Type.UNION if field.schema.getTypes.asScala.toList.exists(_.isNullable) => false
        case Schema.Type.NULL                                                               => false
        case _                                                                              => true
      }

    keyFields.map(field =>
      validate(streamType == StreamTypeV2.Event || fieldIsNotNull(field), KeyHasNullableFieldError(field.name(), field.schema()))
    ).pure
  }

  private def checkForDefaultNullableValueFields(valueFields: List[Schema.Field], streamType: StreamTypeV2): F[List[ValidationChain]] = {
    def validateIfFieldIsNullable(field: Schema.Field): Boolean =
      field.schema().getType match {
        case Schema.Type.UNION if field.schema().getTypes.asScala.toList.exists(_.isNullable) && Option(field.defaultVal()).isEmpty => false
        case _ => true
      }

    valueFields.map(field =>
      validate(validateIfFieldIsNullable(field), NullableFieldWithoutDefaultValueError(field.name(), field.schema()))
    ).pure
  }

  private def checkForMismatches(keyFields: List[Schema.Field], valueFields: List[Schema.Field]): F[List[ValidationChain]] =
    keyFields.flatMap { keyField =>
      valueFields.map { valueField =>
        validate(keyField.name() != valueField.name() || keyField.schema().equals(valueField.schema()),
          IncompatibleKeyAndValueFieldNamesError(keyField.name(), keyField.schema(), valueField.schema()))
      }
    }.pure
}

object KeyAndValueSchemaV2Validator {
  def make[F[_]: Sync](schemaRegistry: SchemaRegistry[F]): KeyAndValueSchemaV2Validator[F] =
    new KeyAndValueSchemaV2Validator(schemaRegistry)
}
