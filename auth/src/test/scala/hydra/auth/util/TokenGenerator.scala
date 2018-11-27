package hydra.auth.util

import java.util.UUID

import hydra.auth.persistence.TokenInfoRepository.TokenInfo

object TokenGenerator {
  def generateTokenInfo: TokenInfo = {
    TokenInfo(UUID.randomUUID().toString,
      Set("resourceA", "resourceB"))
  }
}
