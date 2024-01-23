package hydra.kafka.programs

import cats.data.{NonEmptyChain, Validated}
import cats.effect.Sync
import cats.syntax.all._
import hydra.avro.convert.IsoDate
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.model.{AdditionalValidation, RequiredField, SchemaAdditionalValidation, Schemas, SkipValidation, StreamTypeV2, TopicMetadataV2Request}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.programs.TopicSchemaError._
import org.apache.avro.{Schema, SchemaBuilder}
import RequiredFieldStructures._
import hydra.common.validation.Validator
import hydra.common.validation.Validator.{ValidationChain, valid}
import hydra.kafka.algebras.MetadataAlgebra
import hydra.kafka.model.RequiredField.RequiredField

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter
import scala.language.higherKinds

class KeyAndValueSchemaV2Validator[F[_]: Sync] private (schemaRegistry: SchemaRegistry[F],
                                                        metadataAlgebra: MetadataAlgebra[F]) extends Validator {

  def validate(request: TopicMetadataV2Request, subject: Subject, withRequiredFields: Boolean,
               maybeSkipValidations: Option[List[SkipValidation]] = None): F[Unit] = {

    val skipValidations = maybeSkipValidations.getOrElse(List.empty)
    if (skipValidations.contains(SkipValidation.all)) {
      return ().pure
    }

    for {
      metadata <- metadataAlgebra.getMetadataFor(subject)
      validateDefaultInRequiredField = AdditionalValidation.isPresent(metadata, SchemaAdditionalValidation.defaultInRequiredField)
      schemas = request.schemas
      _ <- (Option(schemas.key).map(_.getType).getOrElse(Schema.Type.NULL), Option(schemas.value).map(_.getType).getOrElse(Schema.Type.NULL)) match {
        case (Schema.Type.RECORD, Schema.Type.RECORD) =>
          validateRecordRecordTypeSchemas(schemas, subject, request.streamType, withRequiredFields, validateDefaultInRequiredField, skipValidations)
        case (Schema.Type.STRING, Schema.Type.RECORD) if request.tags.contains("KSQL") =>
          validateKSQLSchemas(schemas, subject, request.streamType, skipValidations)
        case _ => resultOf(Validated.Invalid(NonEmptyChain.one(InvalidSchemaTypeError)))
      }
    } yield()
  }

  private def validateKSQLSchemas(schemas: Schemas, subject: Subject, streamType: StreamTypeV2,
                                  skipValidations: List[SkipValidation]): F[Unit] = {
    val concoctedKeyFields = SchemaBuilder
      .record("uselessRecord") //This is a useless record whose only purpose is to transform the string key into a list of fields.
      .fields()
      .name(schemas.key.getName)
      .`type`()
      .stringType()
      .noDefault()
      .endRecord().getFields.asScala.toList

    val valueFields = schemas.value.getFields.asScala.toList

    val doKeySchemaEvolutionValidation = !skipValidations.contains(SkipValidation.keySchemaEvolution)
    val doValueSchemaEvolutionValidation = !skipValidations.contains(SkipValidation.valueSchemaEvolution)
    val doSameFieldsTypeMismatchInKeyValueSchemasValidation = !skipValidations.contains(SkipValidation.sameFieldsTypeMismatchInKeyValueSchemas)
    val doMissingDefaultInNullableFieldsOfValueSchemaValidation = !skipValidations.contains(SkipValidation.missingDefaultInNullableFieldsOfValueSchema)
    val doUnsupportedLogicalTypeFieldsInKeySchemaValidation = !skipValidations.contains(SkipValidation.unsupportedLogicalTypeFieldsInKeySchema)
    val doUnsupportedLogicalTypeFieldsInValueSchemaValidation = !skipValidations.contains(SkipValidation.unsupportedLogicalTypeFieldsInValueSchema)

    val validators = for {
        keySchemaEvolutionValidationResult         <- validateKeySchemaEvolution(schemas, subject, doKeySchemaEvolutionValidation)
        valueSchemaEvolutionValidationResult       <- validateValueSchemaEvolution(schemas, subject, doValueSchemaEvolutionValidation)
        defaultNullableValueFieldsValidationResult <- checkForDefaultNullableValueFields(valueFields, doMissingDefaultInNullableFieldsOfValueSchemaValidation)
        unsupportedLogicalTypesKey                 <- checkForUnsupportedLogicalType(concoctedKeyFields, doUnsupportedLogicalTypeFieldsInKeySchemaValidation)
        unsupportedLogicalTypesValues              <- checkForUnsupportedLogicalType(valueFields, doUnsupportedLogicalTypeFieldsInValueSchemaValidation)
        mismatchesValidationResult                 <- checkForMismatches(concoctedKeyFields, valueFields, doSameFieldsTypeMismatchInKeyValueSchemasValidation)
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

  private def validateRecordRecordTypeSchemas(schemas: Schemas,
                                              subject: Subject,
                                              streamType: StreamTypeV2,
                                              withRequiredFields: Boolean,
                                              validateDefaultInRequiredField: Boolean,
                                              skipValidations: List[SkipValidation]): F[Unit] = {
    val keyFields   = schemas.key.getFields.asScala.toList
    val valueFields = schemas.value.getFields.asScala.toList

    val doEmptyKeyFieldsValidation = !skipValidations.contains(SkipValidation.emptyKeyFields)
    val doKeySchemaEvolutionValidation = !skipValidations.contains(SkipValidation.keySchemaEvolution)
    val doValueSchemaEvolutionValidation = !skipValidations.contains(SkipValidation.valueSchemaEvolution)
    val doSameFieldsTypeMismatchInKeyValueSchemasValidation = !skipValidations.contains(SkipValidation.sameFieldsTypeMismatchInKeyValueSchemas)
    val doNullableFieldsInKeySchemaValidation = !skipValidations.contains(SkipValidation.nullableFieldsInKeySchema)
    val doMissingDefaultInNullableFieldsOfValueSchemaValidation = !skipValidations.contains(SkipValidation.missingDefaultInNullableFieldsOfValueSchema)
    val doUnsupportedLogicalTypeFieldsInKeySchemaValidation = !skipValidations.contains(SkipValidation.unsupportedLogicalTypeFieldsInKeySchema)
    val doUnsupportedLogicalTypeFieldsInValueSchemaValidation = !skipValidations.contains(SkipValidation.unsupportedLogicalTypeFieldsInValueSchema)

    val validators = for {
      keyFieldsValidationResult                  <- validateKeyFields(keyFields, doEmptyKeyFieldsValidation)
      keySchemaEvolutionValidationResult         <- validateKeySchemaEvolution(schemas, subject, doKeySchemaEvolutionValidation)
      valueSchemaEvolutionValidationResult       <- validateValueSchemaEvolution(schemas, subject, doValueSchemaEvolutionValidation)
      validateRequiredKeyFieldsResult            <- if (withRequiredFields) {
          validateRequiredKeyFields(schemas.key, streamType, validateDefaultInRequiredField, skipValidations)
        } else {
          Nil.pure
        }
      validateRequiredValueFieldsResult          <- if (withRequiredFields) {
          validateRequiredValueFields(schemas.value, streamType, validateDefaultInRequiredField, skipValidations)
        } else {
          Nil.pure
        }
      mismatchesValidationResult                 <- checkForMismatches(keyFields, valueFields, doSameFieldsTypeMismatchInKeyValueSchemasValidation)
      nullableKeyFieldsValidationResult          <- checkForNullableKeyFields(keyFields, streamType, doNullableFieldsInKeySchemaValidation)
      defaultNullableValueFieldsValidationResult <- checkForDefaultNullableValueFields(valueFields, doMissingDefaultInNullableFieldsOfValueSchemaValidation)
      unsupportedLogicalTypesKey                 <- checkForUnsupportedLogicalType(keyFields, doUnsupportedLogicalTypeFieldsInKeySchemaValidation)
      unsupportedLogicalTypesValues              <- checkForUnsupportedLogicalType(valueFields, doUnsupportedLogicalTypeFieldsInValueSchemaValidation)
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

  private def checkForUnsupportedLogicalType(fields: List[Schema.Field], doValidation: Boolean): F[List[ValidationChain]] =
    if (doValidation) {
      fields.map { field =>
        validate(!getLogicalType(field.schema()).contains(IsoDate.IsoDateLogicalTypeName),
          UnsupportedLogicalType(field, getLogicalType(field.schema()).getOrElse("")))
      }.pure
    } else {
      List(valid).pure
    }

  private def validateKeyFields(keyFields: List[Schema.Field], doValidation: Boolean): F[ValidationChain] =
    if (doValidation) {
      validate(keyFields.nonEmpty, TopicSchemaError.KeyIsEmptyError).pure
    } else {
      valid.pure
    }

  private def validateKeySchemaEvolution(schemas: Schemas, subject: Subject, doValidation: Boolean): F[List[ValidationChain]] =
    if (doValidation) {
      validateSchemaEvolution(schemas, subject + "-key")
    } else {
      List(valid).pure
    }

  private def validateValueSchemaEvolution(schemas: Schemas, subject: Subject, doValidation: Boolean): F[List[ValidationChain]] =
    if (doValidation) {
      validateSchemaEvolution(schemas, subject + "-value")
    } else {
      List(valid).pure
    }

  private def validateSchemaEvolution(schemas: Schemas, subject: String): F[List[ValidationChain]] =
    schemaRegistry.getLatestSchemaBySubject(subject).map {
      case Some(latestSchema) =>
        val schema = if (subject.endsWith("-key")) schemas.key else schemas.value
        checkForIllegalLogicalTypeEvolutions(latestSchema, schema, schema.getName)
      case _ => List(Validator.valid)
    }

  private def validateRequiredKeyFields(keySchema: Schema, streamType: StreamTypeV2, validateDefaultInRequiredField: Boolean,
                                        skipValidations: List[SkipValidation]): F[List[ValidationChain]] =
    validateRequiredFields(isKey = true, keySchema, streamType, validateDefaultInRequiredField, skipValidations)

  private def validateRequiredValueFields(valueSchema: Schema, streamType: StreamTypeV2, validateDefaultInRequiredField: Boolean,
                                          skipValidations: List[SkipValidation]): F[List[ValidationChain]] =
    validateRequiredFields(isKey = false, valueSchema, streamType, validateDefaultInRequiredField, skipValidations)

  private def validateRequiredFields(isKey: Boolean, schema: Schema, streamType: StreamTypeV2,
                                     validateDefaultInRequiredField: Boolean, skipValidations: List[SkipValidation]): F[List[ValidationChain]] = {
    val doRequiredDocFieldValidation = !skipValidations.contains(SkipValidation.requiredDocField)
    val doRequiredCreatedAtFieldValidation = !skipValidations.contains(SkipValidation.requiredCreatedAtField)
    val doRequiredUpdatedAtFieldValidation = !skipValidations.contains(SkipValidation.requiredUpdatedAtField)

    streamType match {
      case (StreamTypeV2.Entity | StreamTypeV2.Event) =>
        if (isKey) {
          List(validateRequiredField(doRequiredDocFieldValidation, RequiredField.DOC, isKey, schema, streamType.toString)).pure
        } else {
          List(
            validateRequiredField(doRequiredDocFieldValidation, RequiredField.DOC, isKey, schema, streamType.toString),
            validateRequiredField(doRequiredCreatedAtFieldValidation, RequiredField.CREATED_AT, isKey, schema, streamType.toString),
            validateRequiredField(doRequiredUpdatedAtFieldValidation, RequiredField.UPDATED_AT, isKey, schema, streamType.toString),
            validateDefaultFieldInRequiredField(doRequiredCreatedAtFieldValidation, RequiredField.CREATED_AT,
              validateDefaultInRequiredField, schema, streamType.toString),
            validateDefaultFieldInRequiredField(doRequiredUpdatedAtFieldValidation, RequiredField.UPDATED_AT,
              validateDefaultInRequiredField, schema, streamType.toString)
          ).pure
        }
      case _ =>
        List(validateRequiredField(doRequiredDocFieldValidation, RequiredField.DOC, isKey, schema, streamType.toString)).pure
    }
  }

  private def validateRequiredField(doValidation: Boolean, requiredField: RequiredField, isKey: Boolean, schema: Schema, streamType: String): ValidationChain =
    if (doValidation) {
      val result = requiredField match {
        case RequiredField.DOC         => docFieldValidator(schema)
        case RequiredField.CREATED_AT  => createdAtFieldValidator(schema)
        case RequiredField.UPDATED_AT  => updatedAtFieldValidator(schema)
      }
      validate(result, getFieldMissingError(isKey, requiredField, schema, streamType))
    } else {
      valid
    }

  private def validateDefaultFieldInRequiredField(doValidation: Boolean,
                                                  requiredField: RequiredField,
                                                  validateDefaultInRequiredField: Boolean,
                                                  schema: Schema,
                                                  streamType: String): ValidationChain =
    if (doValidation) {
      validate(defaultFieldOfRequiredFieldValidator(schema, requiredField, validateDefaultInRequiredField),
        RequiredSchemaValueFieldWithDefaultValueError(requiredField, schema, streamType))
    } else {
      valid
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

  private def checkForNullableKeyFields(keyFields: List[Schema.Field], streamType: StreamTypeV2,
                                        doValidation: Boolean): F[List[ValidationChain]] = {
    def fieldIsNotNull(field: Schema.Field): Boolean =
      field.schema().getType match {
        case Schema.Type.UNION if field.schema.getTypes.asScala.toList.exists(_.isNullable) => false
        case Schema.Type.NULL                                                               => false
        case _                                                                              => true
      }

    if (doValidation) {
      keyFields.map(field =>
        validate(streamType == StreamTypeV2.Event || fieldIsNotNull(field), KeyHasNullableFieldError(field.name(), field.schema()))
      ).pure
    } else {
      List(valid).pure
    }
  }

  private def checkForDefaultNullableValueFields(valueFields: List[Schema.Field], doValidation: Boolean): F[List[ValidationChain]] = {
    def validateIfFieldIsNullable(field: Schema.Field): Boolean =
      field.schema().getType match {
        case Schema.Type.UNION if field.schema().getTypes.asScala.toList.exists(_.isNullable) && Option(field.defaultVal()).isEmpty => false
        case _ => true
      }

    if (doValidation) {
      valueFields.map(field =>
        validate(validateIfFieldIsNullable(field), NullableFieldWithoutDefaultValueError(field.name(), field.schema()))
      ).pure
    } else {
      List(valid).pure
    }
  }

  private def checkForMismatches(keyFields: List[Schema.Field], valueFields: List[Schema.Field],
                                 doValidation: Boolean): F[List[ValidationChain]] =
    if (doValidation) {
      keyFields.flatMap { keyField =>
        valueFields.map { valueField =>
          validate(keyField.name() != valueField.name() || keyField.schema().equals(valueField.schema()),
            IncompatibleKeyAndValueFieldNamesError(keyField.name(), keyField.schema(), valueField.schema()))
        }
      }.pure
    } else {
      List(valid).pure
    }
}

object KeyAndValueSchemaV2Validator {
  def make[F[_]: Sync](schemaRegistry: SchemaRegistry[F], metadataAlgebra: MetadataAlgebra[F]): KeyAndValueSchemaV2Validator[F] =
    new KeyAndValueSchemaV2Validator(schemaRegistry, metadataAlgebra)
}
