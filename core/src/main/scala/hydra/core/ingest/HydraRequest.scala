package hydra.core.ingest

import hydra.core.transport.ValidationStrategy.Strict
import hydra.core.transport.{AckStrategy, ValidationStrategy}

/**
  * We are mimicking Kafka's design of having a null payload be a marker for deletion.
  *
  * Created by alexsilva on 12/3/16.
  */
case class HydraRequest(
    correlationId: String = CorrelationIdBuilder.generate(),
    payload: String,
    clientId: Option[String] = None,
    metadata: Map[String, String] = Map.empty,
    validationStrategy: ValidationStrategy = Strict,
    ackStrategy: AckStrategy = AckStrategy.NoAck
) {

  def metadataValue(name: String): Option[String] = {
    metadata
      .get(name)
      .orElse(metadata.find(_._1.equalsIgnoreCase(name)).map(_._2))
  }

  /**
    * Returns true if a metadata header with the given name exists and matches the given value
    *
    * @param name
    * @param value
    * @return
    */
  def metadataValueEquals(name: String, value: String): Boolean = {
    metadataValue(name).exists(_.equals(value))
  }

  def withCorrelationId(correlationId: String) =
    copy(correlationId = correlationId)

  def withMetadata(meta: (String, String)*) =
    copy(metadata = this.metadata ++ meta)

  def hasMetadata(key: String): Boolean =
    metadata.exists(_._1.equalsIgnoreCase(key))

  def withAckStrategy(strategy: AckStrategy) = copy(ackStrategy = strategy)

  def withValidationStratetegy(validationStrategy: ValidationStrategy) =
    copy(validationStrategy = validationStrategy)
}
