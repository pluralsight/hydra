package hydra.core.ingest

import hydra.core.transport.ValidationStrategy.Strict
import hydra.core.transport.{AckStrategy, RetryStrategy, ValidationStrategy}

import scala.util.Random

/**
  * Created by alexsilva on 12/3/16.
  */
case class HydraRequest(correlationId: String = Random.alphanumeric.take(8).mkString,
                        payload: String,
                        metadata: Seq[HydraRequestMetadata] = Seq.empty,
                        params: Map[String, Any] = Map.empty,
                        retryStrategy: RetryStrategy = RetryStrategy.Fail,
                        validationStrategy: ValidationStrategy = Strict,
                        ackStrategy: AckStrategy = AckStrategy.None) {

  def metadataValue(name: String): Option[String] = {
    metadata.find(m => m.name.equalsIgnoreCase(name)) match {
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
    copy(metadata = this.metadata ++ meta.map(m => HydraRequestMetadata(m._1, m._2)))

  def withRetryStrategy(retryStrategy: RetryStrategy) =
    copy(retryStrategy = retryStrategy)

  def withValidationStratetegy(validationStrategy: ValidationStrategy) =
    copy(validationStrategy = validationStrategy)

  def withParams(params: (String, Any)*) = copy(params = this.params ++ params.toMap)
}


case class HydraRequestMetadata(name: String, value: String)