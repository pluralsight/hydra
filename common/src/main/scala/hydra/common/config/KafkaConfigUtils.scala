package hydra.common.config

import cats.implicits._
import ciris.{ConfigValue, env}
import org.apache.kafka.common.config.SaslConfigs

import scala.language.reflectiveCalls

object KafkaConfigUtils {


  final case class SchemaRegistrySecurityConfig(
                                                 basicAuthCredSource: Option[String],
                                                 basicAuthUserInfo: Option[String]
                                               ) {

    lazy val toConfigMap: Map[String, String] = List(
      basicAuthCredSource.map("basic.auth.credentials.source" -> _),
      basicAuthUserInfo.map("basic.auth.user.info" -> _)
    ).flatten.toMap

  }

  //TODO: should be moved to AppConfig after V1 Deprecation
  val schemaRegistrySecurityConfig: ConfigValue[SchemaRegistrySecurityConfig] =
    (
      env("HYDRA_SCHEMA_BASIC_AUTH_CREDENTIALS_SOURCE").as[String].option,
      env("HYDRA_SCHEMA_BASIC_AUTH_USER_INFO").as[String].option,
      ).parMapN(SchemaRegistrySecurityConfig)


  final case class KafkaClientSecurityConfig(
                                              securityProtocol: Option[String],
                                              saslMechanism: Option[String],
                                              saslJaasConfig: Option[String],
                                            ) {

    lazy val toConfigMap: Map[String, String] = List(
      securityProtocol.map("security.protocol" -> _),
      saslMechanism.map(SaslConfigs.SASL_MECHANISM -> _),
      saslJaasConfig.map(SaslConfigs.SASL_JAAS_CONFIG -> _)
    ).flatten.toMap

  }


  //TODO: should be moved to AppConfig after V1 Deprecation
  val kafkaClientSecurityConfig: ConfigValue[KafkaClientSecurityConfig] =
    (
      env("HYDRA_KAFKA_SECURITY_PROTOCOL").as[String].option,
      env("HYDRA_KAFKA_SASL_MECHANISM").as[String].option,
      env("HYDRA_KAFKA_SASL_JAAS_CONFIG").as[String].option
      ).parMapN(KafkaClientSecurityConfig)


  val kafkaSecurityEmptyConfig: KafkaClientSecurityConfig = KafkaClientSecurityConfig(None, None, None)


  type KafkaPropertiesBuilder[T] = {

    def withProperties(properties : Map[String, String]) : T
  }


  implicit class KafkaConfigurationAdder[T <: KafkaPropertiesBuilder[T]](configBuilder: T) {

    def withKafkaSecurityConfigs(kafkaClientSecurityConfig: KafkaClientSecurityConfig): T = {
      configBuilder.withProperties(kafkaClientSecurityConfig.toConfigMap)
    }

  }

}
