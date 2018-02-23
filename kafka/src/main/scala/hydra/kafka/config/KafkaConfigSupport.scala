/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.kafka.config

import java.util.Properties

import com.typesafe.config._
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 11/30/15.
  */
trait KafkaConfigSupport extends ConfigSupport {

  import KafkaConfigSupport._

  protected[config] val kafkaConfig = applicationConfig.withOnlyPath("kafka")

  /**
    * The key of the map is the topic format (avro, json, etc.) and the value are the properties, which is a merge
    * of the default properties and the format-specific properties.
    */
  lazy val kafkaProducerFormats: Map[String, Config] = loadProducerFormats(kafkaConfig)

  lazy val kafkaConsumerFormats: Map[String, Config] = loadConsumerFormats(kafkaConfig)

  val zkString = kafkaConfig.getString("kafka.consumer.zookeeper.connect")

  val bootstrapServers = kafkaConfig.getString("kafka.producer.bootstrap.servers")

  val schemaRegistryUrl = applicationConfig.getString("schema.registry.url")

  /**
    * Allows specific topic config parameters (schema name, etc.) to be configured by topic name, message types, ack,
    * etc.
    * Allows clients to specify a topic name and get all the config.
    */
  lazy val topicConfigs: Map[String, Config] = {
    val topics = applicationConfig.get[ConfigObject]("kafka.topics")
      .valueOrElse(ConfigFactory.empty().root()).entrySet.asScala.toSeq
    Map(topics map (entry => {
      val cfgObj = entry.getValue.asInstanceOf[ConfigObject].toConfig
      val topicFormat = cfgObj.get[String]("format").valueOrElse("avro")
      val topicConfig = kafkaProducerFormats(topicFormat)
      entry.getKey -> cfgObj.withFallback(topicConfig)
    }): _*)
  }

  def topicConsumerConfigs(topicName: String, fallbackFormat: Option[String] = None): Config = {
    topicConfigs.get(topicName) match {
      case Some(p) => p
      case None => kafkaConsumerFormats(fallbackFormat.getOrElse("string"))
    }
  }

  lazy val kafkaConsumerDefaults: Config = loadConsumerDefaults(kafkaConfig).withFallback(kafkaConsumerFormats("string"))

  /**
    * Allows the configuration of a Kafka consumer 'on the fly', meaning that it doesn't have to be present in
    * any configuration file.
    * The properties passed are specific to the topic type; i.e, an 'avro' topic should have the 'avro.schema'
    * property defined.
    *
    * No property validation is done at this level.
    *
    * @param overrideProps
    */
  def consumerConfig(topicFormat: String, overrideProps: Config = ConfigFactory.empty): Properties = {
    Option(applicationConfig.getObject("kafka.formats").get(topicFormat)) match {
      case Some(c) => {
        val cfgObj = c.asInstanceOf[ConfigObject].toConfig
        val config = overrideProps.withFallback(kafkaConsumerFormats(topicFormat)).withFallback(cfgObj).atKey("kafka")
        val props = new Properties()
        props.putAll(ConfigSupport.toMap(config.getObject("kafka")).asJava)
        props
      }
      case None => new Properties()
    }
  }
}

object KafkaConfigSupport extends ConfigSupport with LoggingAdapter {

  val schemaRegistryUrl = applicationConfig.getString("schema.registry.url")

  private val schemaRegistryConfig = ConfigFactory.parseString(s"""schema.registry.url="$schemaRegistryUrl"""")

  lazy val configClients: Map[String, Map[String, String]] = clients(applicationConfig)

  def clients(config: Config): Map[String, Map[String, String]] = {
    val producerDefaults = config.get[Config]("kafka.producer")
      .valueOrElse(ConfigFactory.empty)
      .withFallback(schemaRegistryConfig)

    val clients = config.get[Config]("kafka.clients").valueOrElse(ConfigFactory.empty)
    clients.root().asScala.map { case (clientId, config) =>
      val clientIdCfg = ConfigFactory.parseString(s"client.id=$clientId")
      val clientCfg = config.withFallback(clientIdCfg).withFallback(producerDefaults)
      clientId -> ConfigSupport.toMap(clientIdCfg
        .withFallback(clientCfg)).mapValues(_.toString)
    }.toMap
  }

  def loadConsumerFormats(cfg: Config): Map[String, Config] = {
    val formats = cfg.getObject("kafka.formats").entrySet.asScala.toSeq
    val consumerDefaults = loadConsumerDefaults(cfg)
    Map(formats.map { entry => {
      val c = entry.getValue.asInstanceOf[ConfigObject].toConfig
      val p = c.withFallback(consumerDefaults)
      (entry.getKey, p)
    }
    }: _*)
  }

  def loadProducerFormats(cfg: Config): Map[String, Config] = {
    val producerDefaults = cfg.get[Config]("kafka.producer").valueOrElse(ConfigFactory.empty)
      .withFallback(schemaRegistryConfig)
    val formats = cfg.getObject("kafka.formats").entrySet.asScala.toSeq
    Map(formats.map { entry => {
      val c = entry.getValue.asInstanceOf[ConfigObject].toConfig
      val p = c.withFallback(producerDefaults)
      (entry.getKey, p)
    }
    }: _*)
  }

  def loadConsumerDefaults(cfg: Config): Config =
    cfg.get[Config]("kafka.consumer").valueOrElse(ConfigFactory.empty).withFallback(schemaRegistryConfig)

}
