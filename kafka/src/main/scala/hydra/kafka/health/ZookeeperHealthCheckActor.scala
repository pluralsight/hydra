package hydra.kafka.health

import akka.actor.Actor
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.vonnagy.service.container.health.{CheckHealth, GetHealth, HealthInfo}
import configs.syntax._
import org.apache.zookeeper.ZooKeeper

import scala.concurrent.duration._

/**
  * Created by alexsilva on 9/30/16.
  */
class ZookeeperHealthCheckActor extends Actor with ClusterHealthCheck {

  val interval = applicationConfig.get[FiniteDuration]("zk.health_check.interval").valueOrElse(10.seconds)

  private val zkConnect = kafkaConfig.getString("settings.zookeeper.connect")

  private val currentHealth: HealthInfo = HealthInfo("Zookeeper", details = "")

  private val mapper = new ObjectMapper()

  implicit val ec = context.dispatcher

  context.system.scheduler.schedule(interval, interval, self, CheckHealth)

  override def receive: Receive = {
    case GetHealth => sender ! currentHealth
  }

  def checkHealth(): Unit = {
    import scala.collection.JavaConverters._
    val zk = new ZooKeeper(zkConnect, 10000, null);
    val brokerList = zk.getChildren("/brokers/ids", false).asScala
      .map(s => mapper.readValue(zk.getData(s"/brokers/ids/$s", false, null), classOf[Map[Any, Any]]))

    println(brokerList)
  }
}

