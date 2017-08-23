package hydra.kafka.health

import akka.actor.Actor
import com.github.vonnagy.service.container.health.{GetHealth, HealthInfo, HealthState}
import configs.syntax._
import hydra.kafka.health.KafkaHealthCheckActor.GetKafkaHealth
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
  * Created by alexsilva on 9/30/16.
  */
class KafkaHealthCheckActor extends Actor with ClusterHealthCheck {

  val interval = applicationConfig.get[FiniteDuration]("kafka.health_check.interval").valueOrElse(10.seconds)

  val healthCheckTopic = applicationConfig.get[String]("kafka.health_check.topic").valueOrElse("__hydra_health_check")

  @volatile
  private var currentHealth: HealthInfo = HealthInfo("Kafka", details = "")

  lazy val producer = new KafkaProducer[String, String](toMap(producerConfig).asJava)

  implicit val ec = context.dispatcher

  context.system.scheduler.schedule(interval, interval, self, GetKafkaHealth)

  override def receive: Receive = {
    case GetHealth => sender ! currentHealth
    case GetKafkaHealth => sendTestMessage()
  }

  override def postStop(): Unit = producer.close()


  private def maybePublish(newHealth: HealthInfo) = {
    if (newHealth.state != currentHealth.state || currentHealth.state == HealthState.CRITICAL) {
      context.system.eventStream.publish(newHealth)
    }
  }

  private def sendTestMessage(): Unit = {
    val time = System.currentTimeMillis().toString
    val record = new ProducerRecord[String, String](healthCheckTopic, time)
    producer.send(record, (metadata: RecordMetadata, e: Exception) => {
      val h = if (e != null) {
        critical(e)
      } else {
        HealthInfo("Kafka", details = s"Metadata request succeeded at ${DateTime.now.toString()}.")
      }
      maybePublish(h)
      currentHealth = h
    })
  }

  private def critical(e: Throwable) =
    HealthInfo("Kafka", state = HealthState.CRITICAL,
      details = s"Metadata request failed at ${DateTime.now.toString()}. Error: ${e.getMessage}",
      extra = Some(s"Kafka: ${applicationConfig.get[String]("kafka.bootstrap.servers").valueOrElse("?")}"))
}

object KafkaHealthCheckActor {

  case object GetKafkaHealth

}
