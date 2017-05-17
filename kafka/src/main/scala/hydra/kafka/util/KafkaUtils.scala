package hydra.kafka.util

import hydra.common.config.ConfigSupport
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient

import scala.util.Try

/**
  * Created by alexsilva on 5/17/17.
  */
object KafkaUtils extends ConfigSupport {

  val zkString = applicationConfig.getString("kafka.consumer.zookeeper.connect")

  println(zkString)

  val zkUtils = Try(new ZkClient(zkString, 5000)).map(ZkUtils(_, false))

  def topicExists(name: String): Boolean = {
    zkUtils.map(AdminUtils.topicExists(_, name)).getOrElse(false)
  }
}
