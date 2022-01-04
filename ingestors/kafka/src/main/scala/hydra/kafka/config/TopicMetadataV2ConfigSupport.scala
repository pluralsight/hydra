package hydra.kafka.config

import hydra.common.Settings.applicationConfig

object TopicMetadataV2ConfigSupport {
  val AllowedOrganizations= applicationConfig.getString("subject.allowed.organizations")
}
