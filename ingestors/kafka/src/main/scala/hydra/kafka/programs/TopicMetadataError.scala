package hydra.kafka.programs

import hydra.common.validation.ValidationError
import hydra.kafka.model.SubDataClassification

sealed trait TopicMetadataError extends ValidationError

object TopicMetadataError {

  case class InvalidSubDataClassificationTypeError(dataClassification: String,
                                                   subDataClassification: String,
                                                   supportedValues: Seq[SubDataClassification]) extends TopicMetadataError {

    override def message: String = {
      val validValues = s"Valid value is ${if (supportedValues.size == 1) s"'${supportedValues.head}'" else s"one of [${supportedValues.mkString(", ")}]"}."
      s"'$subDataClassification' is not a valid SubDataClassification value for '$dataClassification' DataClassification. $validValues"
    }
  }
}
