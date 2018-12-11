package hydra.auth.util

import java.util.UUID

import hydra.auth.persistence.AuthRepository.TokenInfo

object TokenGenerator {
  def generateTokenInfo: TokenInfo = {
    TokenInfo(UUID.randomUUID().toString, 1,
      Set("resourceA", "resourceB"))
  }
}
