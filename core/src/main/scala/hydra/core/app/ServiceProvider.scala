package hydra.core.app

import akka.actor.Props

trait ServiceProvider {
  def services: Seq[(String, Props)]
}
