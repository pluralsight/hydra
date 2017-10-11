package hydra.kafka.health

import java.io.Closeable

import akka.actor.{Actor, Props}
import com.github.vonnagy.service.container.health.{HealthInfo, HealthState}
import hydra.common.util.Resource._
import org.I0Itec.zkclient.ZkClient
import org.joda.time.DateTime

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 9/30/16.
  */
class ZookeeperHealthCheckActor(zookeeper: String, val interval: FiniteDuration) extends Actor with ClusterHealthCheck {

  override val name = s"Zookeeper [$zookeeper]"

  override def checkHealth(): Future[HealthInfo] = {
    Future {
      import scala.collection.JavaConverters._
      val healthInfo = withZookeeper { czk =>
        czk.getChildren("/").asScala.headOption
          .map(_ => HealthInfo(name, details = s"Zookeeper request succeeded at ${DateTime.now.toString()}."))
          .getOrElse(HealthInfo(name, state = HealthState.CRITICAL,
            details = s"Empty znode /brokers/topics reported at ${DateTime.now.toString()}."))
      }
      healthInfo
    }
  }

  def withZookeeper(body: (ZkClient) => HealthInfo): HealthInfo = {
    Try(new ZkClient(zookeeper, 1000, 1000)) match {
      case Success(zk) => using(CloseableZkClient(zk))(c => body(c.zk))

      case Failure(ex) => HealthInfo(name, state = HealthState.CRITICAL, details = ex.getMessage)
    }
  }
}

private case class CloseableZkClient(zk: ZkClient) extends Closeable {
  override def close() = zk.close()
}

object ZookeeperHealthCheckActor {
  def props(zkString: String, interval: FiniteDuration): Props =
    Props(classOf[ZookeeperHealthCheckActor], zkString, interval)

}
