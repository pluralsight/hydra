/*
 * Copyright (C) 2016 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package hydra.common.logging

import akka.actor.Actor
import akka.event.LogSource
import akka.event.slf4j.{Logger, SLF4JLogging}

trait ActorLoggingAdapter {
  this: Actor =>

  val logSrc = LogSource(self, context.system)

  @transient
  lazy val log = Logger(logSrc._2, logSrc._1)

}

/**
  * Use this trait in your class so that there is logging support
  */
trait LoggingAdapter extends SLF4JLogging
