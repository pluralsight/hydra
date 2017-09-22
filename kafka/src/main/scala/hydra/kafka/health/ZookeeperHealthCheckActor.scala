package hydra.kafka.health

import java.io.Closeable

import akka.actor.Actor
import com.github.vonnagy.service.container.health.HealthInfo
import configs.syntax._
import org.I0Itec.zkclient.ZkClient
import org.joda.time.DateTime
import hydra.common.util.Resource._

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Created by alexsilva on 9/30/16.
  */
class ZookeeperHealthCheckActor extends Actor with ClusterHealthCheck {

  val interval = applicationConfig.get[FiniteDuration]("health.zookeeper.interval").valueOrElse(10.seconds)

  override val name = s"Zookeeper [$zkString]"

  override def checkHealth(): Future[HealthInfo] = {
    Future {
      import scala.collection.JavaConverters._
      using(CloseableZkClient(new ZkClient(zkString, 1000, 1000))) { czk =>
        czk.zk.getChildren("/brokers/topics").asScala.headOption
          .map(_ => HealthInfo(name, details = s"Zookeeper request succeeded at ${DateTime.now.toString()}."))
          .getOrElse(HealthInfo(name, details = s"Empty znode /brokers/topics reported at ${DateTime.now.toString()}."))
      }
    }
  }
}

private case class CloseableZkClient(zk: ZkClient) extends Closeable {
  override def close() = zk.close()
}

