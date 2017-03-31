package hydra.ingest.ingestors

/**
  * Created by alexsilva on 3/31/17.
  */
abstract class IngestorRef {

}

/**
  * All ActorRefs have a scope which describes where they live. Since it is
  * often necessary to distinguish between local and non-local references, this
  * is the only method provided on the scope.
  */
private[hydra] trait IngestorRefScope {
  def isLocal: Boolean
}

private[hydra] trait LocalRef extends IngestorRefScope {
  final def isLocal = true
}


private[hydra] abstract class InternalIngestorRef extends IngestorRef {
  this: IngestorRefScope =>

}


private[hydra] class LocalIngestorRef extends LocalRef {

}