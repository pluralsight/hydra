package hydra.core.bootstrap

import hydra.core.bootstrap.HealthState.HealthState

/**
  * The health check is used to define a subset of functionality that
  * can be asked of it's state. It can contain optional child checks.
  *
  * @param name  The name of the check
  * @param state The current state of the check (defaults to OK)
  * @param extra Any extra information
  */
case class HealthInfo(
    name: String,
    state: HealthState = HealthState.OK,
    details: String,
    extra: Option[AnyRef] = None,
    checks: List[HealthInfo] = List.empty
)

object HealthState extends Enumeration {
  type HealthState = Value
  val OK, DEGRADED, CRITICAL = Value
}
