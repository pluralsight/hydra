package hydra.sandbox.produce

import hydra.core.transport.{RecordMetadata, RetryStrategy}

/**
  * Created by alexsilva on 4/25/17.
  */
case class FileRecordMetadata(path: String, deliveryId: Long = 0L,
                              retryStrategy: RetryStrategy) extends RecordMetadata
