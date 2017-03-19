package hydra.core.ingest

import hydra.core.produce.ValidationStrategy
import hydra.core.produce.ValidationStrategy.Strict

/**
  * Created by alexsilva on 12/3/16.
  */
case class HydraRequest(label: Option[String] = None,
                        payload: String,
                        metadata: List[HydraRequestMedatata] = List.empty,
                        params: Map[String, Any] = Map.empty,
                        retryStrategy: RetryStrategy = RetryStrategy.Fail,
                        validationStrategy: ValidationStrategy = Strict) {

  def metadataValue(name: String): Option[String] = {
    metadata.find(m => m.name == name) match {
      case Some(md) => Some(md.value)
      case None => None
    }
  }

  /**
    * Returns true if a metadata header with the given name exists and matches the given value
    *
    * @param name
    * @param value
    * @return
    */
  def metadataValueEquals(name: String, value: String): Boolean = {
    metadata.find(_.name.equalsIgnoreCase(name)) match {
      case Some(ua) => ua.value.equals(value)
      case None => false
    }
  }

  def withMetadata(meta: (String, String)*) =
    copy(metadata = this.metadata ::: meta.map(m => HydraRequestMedatata(m._1, m._2)).toList)

  def withRetryStrategy(retryStrategy: RetryStrategy) =
    copy(retryStrategy = retryStrategy)

  def withValidationStratetegy(validationStrategy: ValidationStrategy) =
    copy(validationStrategy = validationStrategy)

  def withParams(params: (String, Any)*) = copy(params = this.params ++ params.toMap)
}


case class HydraRequestMedatata(name: String, value: String)