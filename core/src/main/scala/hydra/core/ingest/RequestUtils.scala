package hydra.core.ingest

/**
  * Created by alexsilva on 3/22/17.
  */
object RequestUtils {
  /**
    * Splits a request with a Json array payload into an array of requests where the payload of each new request
    * is an element of the original array
    *
    * @param request
    * @return
    */
  def split(request: HydraRequest): Seq[HydraRequest] = {
    import spray.json.DefaultJsonProtocol._
    import spray.json._
    if (request.payload.trim.startsWith("[")) {
      val array = request.payload.parseJson.convertTo[JsArray]
      array.elements.map(el => el.compactPrint).map(element => request.copy(payload = element))
    } else {
      Seq(request)
    }
  }
}
