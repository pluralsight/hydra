package hydra.core.ingest

import hydra.core.transport.ValidationStrategy.Strict
import hydra.core.transport.{AckStrategy, DeliveryStrategy, ValidationStrategy}

import scala.util.Random

/**
  * Created by alexsilva on 12/3/16.
  */
case class HydraRequest(correlationId: Long = Random.nextInt(),
                        payload: String,
                        metadata: Map[String, String] = Map.empty,
                        deliveryStrategy: DeliveryStrategy = DeliveryStrategy.BestEffort,
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

  def withAckStrategy(strategy: AckStrategy) = copy(ackStrategy = strategy)

  def withDeliveryStrategy(strategy: DeliveryStrategy) =
    copy(deliveryStrategy = strategy)

  def withValidationStratetegy(validationStrategy: ValidationStrategy) =
    copy(validationStrategy = validationStrategy)
}