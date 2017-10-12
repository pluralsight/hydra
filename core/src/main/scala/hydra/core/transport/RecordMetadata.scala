package hydra.core.transport

/**
  * Created by alexsilva on 2/22/17.
  */
trait RecordMetadata {

  def deliveryId: Long

  def deliveryStrategy: DeliveryStrategy

}
