package hydra.kafka.programs

import cats.effect.Sync
import cats.syntax.all._
import hydra.common.validation.Validator
import hydra.kafka.model.DataClassification._
import hydra.kafka.model.{DataClassification, SubDataClassification, TopicMetadataV2Request}

class TopicMetadataV2Validator[F[_] : Sync] () extends Validator {

  def validate(metadataV2Request: TopicMetadataV2Request): F[Unit] =
    for {
      _ <- validateSubDataClassification(metadataV2Request.dataClassification, metadataV2Request.subDataClassification)
    } yield ()

  private def validateSubDataClassification(dataClassification: DataClassification,
                                            subDataClassification: Option[SubDataClassification]): F[Unit] = {
    (subDataClassification map { sdc =>
      val correctionRequired = collectValidSubDataClassificationValues(dataClassification, sdc)
      resultOf(validate(
        correctionRequired.isEmpty,
        TopicMetadataError.InvalidSubDataClassificationTypeError(dataClassification.entryName, sdc.entryName, correctionRequired)
      ))
    }).getOrElse(().pure)
  }

  private def collectValidSubDataClassificationValues(dc: DataClassification, sdc: SubDataClassification): Seq[SubDataClassification] = dc match {
    case Public       => if (sdc != SubDataClassification.Public) Seq(SubDataClassification.Public) else Seq.empty
    case InternalUse  => if (sdc != SubDataClassification.InternalUseOnly) Seq(SubDataClassification.InternalUseOnly) else Seq.empty
    case Confidential => if (sdc != SubDataClassification.ConfidentialPII) Seq(SubDataClassification.ConfidentialPII) else Seq.empty
    case Restricted   =>
      if (sdc != SubDataClassification.RestrictedFinancial && sdc != SubDataClassification.RestrictedEmployeeData) {
        Seq(SubDataClassification.RestrictedFinancial, SubDataClassification.RestrictedEmployeeData)
      } else {
        Seq.empty
      }
  }
}
