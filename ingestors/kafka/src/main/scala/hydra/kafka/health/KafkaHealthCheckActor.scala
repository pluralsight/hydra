package hydra.kafka.health

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Props}
import com.github.vonnagy.service.container.health.{HealthInfo, HealthState}
import hydra.common.config.ConfigSupport
import hydra.core.protocol.HydraApplicationError
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Created by alexsilva on 9/30/16.
  */
class KafkaHealthCheckActor(bootstrapServers: String, healthCheckTopic: String, val interval: FiniteDuration)
  extends Actor with ClusterHealthCheck {

  override val name = s"Kafka [$bootstrapServers]"

  //TODO: all these intervals should be configurable based on the interval above
  private val producerConfig = Map[String, AnyRef](
    "metadata.fetch.timeout.ms" -> "10000",
    "max.block.ms" -> "10000",
    "bootstrap.servers" -> bootstrapServers,
    "client.id" -> "hydra.health.check",
    "retries" -> "0",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer")

  private val producer = new KafkaProducer[String, String](producerConfig.asJava)

  override def postStop(): Unit = producer.close(java.time.Duration.ofSeconds(5))

  override def checkHealth(): Future[HealthInfo] = {
    val time = System.currentTimeMillis().toString
    val record = new ProducerRecord[String, String](healthCheckTopic, time)
    Future {
      try {
        val md = producer.send(record).get(5, TimeUnit.SECONDS)
        HealthInfo(name, details = s"Metadata request succeeded at ${DateTime.now.toString()} : [${md.topic()}]}.")
      }
      catch {
        case e: Exception => critical(e)
      }
    }

  }

  private def critical(e: Throwable): HealthInfo = {
    context.system.eventStream.publish(HydraApplicationError(e))
    HealthInfo(name, state = HealthState.CRITICAL,
      details = s"Metadata request failed at ${DateTime.now.toString()}. Error: ${e.getMessage}", extra = None)
  }
}

object KafkaHealthCheckActor extends ConfigSupport {

  def props(bootstrapServers: String, healthCheckTopic: String, interval: FiniteDuration) = {
    Props(classOf[KafkaHealthCheckActor], bootstrapServers, healthCheckTopic, interval)
  }
}