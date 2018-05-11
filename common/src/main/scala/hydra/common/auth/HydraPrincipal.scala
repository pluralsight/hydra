package hydra.common.auth

import java.security.Principal

case class HydraPrincipal(name: String, roles: Set[String]) extends Principal {
  override def getName: String = name
}
