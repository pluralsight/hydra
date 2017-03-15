package hydra.core.ingest

/**
  * Created by alexsilva on 3/14/17.
  */
trait RequestFactory[P, D] {
  def createRequest(destination: String, payload: P, data: D): HydraRequest
}
