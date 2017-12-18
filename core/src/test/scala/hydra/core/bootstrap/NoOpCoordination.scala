package hydra.core.bootstrap

import akka.Done
import akka.actor.{ActorSystem, Address}
import de.heikoseeberger.constructr.coordination.Coordination

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class NoOpCoordination(clusterName: String, system: ActorSystem) extends Coordination {
    def getNodes() = Future.successful(Set.empty)

    def lock(self: Address, ttl: FiniteDuration) = Future.successful(true)

    def addSelf(self: Address, ttl: FiniteDuration) = Future.successful(Done)

    def refresh(self: Address, ttl: FiniteDuration) = Future.successful(Done)
}
