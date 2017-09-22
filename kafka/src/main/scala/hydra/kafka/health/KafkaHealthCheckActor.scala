package hydra.kafka.health

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Props}
import com.github.vonnagy.service.container.health.{HealthInfo, HealthState}
import configs.syntax._
import hydra.common.config.ConfigSupport
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Created by alexsilva on 9/30/16.
  */
class KafkaHealthCheckActor(val interval: FiniteDuration) extends Actor with ClusterHealthCheck {

  val healthCheckTopic = applicationConfig.get[String]("kafka.health_check.topic").valueOrElse("__hydra_health_check")

  override val name = s"Kafka [$bootstrapServers]"

  lazy val producer = new KafkaProducer[String, String](toMap(producerConfig).asJava)

  override def postStop(): Unit = producer.close()

  override def checkHealth(): Future[HealthInfo] = {
    val time = System.currentTimeMillis().toString
    val record = new ProducerRecord[String, String](healthCheckTopic, time)
    Future {
      try {
        val md = producer.send(record).get(interval.toMillis, TimeUnit.MILLISECONDS)
        HealthInfo(name, details = s"Metadata request succeeded at ${DateTime.now.toString()} : [${md.topic()}]}.")
      } catch {
        case e: Throwable =>
          critical(e)
      }
    }
  }

  private def critical(e: Throwable): HealthInfo =
    HealthInfo(name, state = HealthState.CRITICAL,
      details = s"Metadata request failed at ${DateTime.now.toString()}. Error: ${e.getMessage}", extra = None)
}

object KafkaHealthCheckActor extends ConfigSupport {

  import configs.syntax._

  def props(interval: Option[FiniteDuration]) = {
    val intv = interval.orElse(applicationConfig.get[FiniteDuration]("kafka.health_check.interval").toOption)
      .getOrElse(20.seconds)
    Props(classOf[KafkaHealthCheckActor], intv)
  }
}