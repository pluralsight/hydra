package hydra.core.ingest

import hydra.core.transport.ValidationStrategy.Strict
import hydra.core.transport.{AckStrategy, RetryStrategy, ValidationStrategy}

import scala.util.Random

/**
  * Created by alexsilva on 12/3/16.
  */
case class HydraRequest(correlationId: Long = Random.nextInt(),
                        payload: String,
                        metadata: Map[String, String] = Map.empty,
                        params: Map[String, Any] = Map.empty,
                        retryStrategy: RetryStrategy = RetryStrategy.Ignore,
                        validationStrategy: ValidationStrategy = Strict,
                        ackStrategy: AckStrategy = AckStrategy.None) {

  def metadataValue(name: String): Option[String] = {
    metadata.get(name).orElse(metadata.find(_._1.equalsIgnoreCase(name)).map(_._2))
  }

  /**
    * Returns true if a metadata header with the given name exists and matches the given value
    *
    * @param name
    * @param value
    * @return
    */
  def metadataValueEquals(name: String, value: String): Boolean = {
    metadataValue(name).map(_.equals(value)).getOrElse(false)
  }

  def withCorrelationId(correlationId: Long) = copy(correlationId = correlationId)

  def withMetadata(meta: (String, String)*) =
    copy(metadata = this.metadata ++ meta)

  def withRetryStrategy(retryStrategy: RetryStrategy) =
    copy(retryStrategy = retryStrategy)

  def withValidationStratetegy(validationStrategy: ValidationStrategy) =
    copy(validationStrategy = validationStrategy)

  def withParams(params: (String, Any)*) = copy(params = this.params ++ params.toMap)
}