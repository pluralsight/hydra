package hydra.core.transport

/**
  * Created by alexsilva on 10/30/15.
  *
  * Generic trait that defines a record that is to be persisted by Hydra.
  *
  * @tparam K The record key type
  * @tparam P The record payload type
  */
trait HydraRecord[+K, +P] {

  def destination: String

  def key: K

  def payload: P

  def ackStrategy: AckStrategy
}
