package hydra.core.ingest

/**
  * Created by alexsilva on 3/17/17.
  */
case class InvalidRequestException(msg: String, request: HydraRequest)
  extends RuntimeException(msg)
