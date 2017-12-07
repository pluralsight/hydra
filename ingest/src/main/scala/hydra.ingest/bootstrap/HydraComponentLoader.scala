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

package hydra.ingest.bootstrap

import java.lang.reflect.Modifier

import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.ingest.Ingestor
import hydra.core.transport.Transport
import org.reflections.Reflections

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 1/12/16.
  */
trait HydraComponentLoader {

  def ingestors: Seq[Class[_ <: Ingestor]]

  def transports: Seq[Class[_ <: Transport]]
}

object ClasspathHydraComponentLoader extends HydraComponentLoader with ConfigSupport with LoggingAdapter {

  import configs.syntax._

  private val ingestorsPkg = applicationConfig.get[List[String]]("ingest.classpath-scan").valueOrElse(List.empty)

  //log.debug(s"Scanning for ingestors in package(s): [${ingestorsPkg.mkString}].")

  private val transportsPkg = applicationConfig.get[List[String]]("transports.classpath-scan").valueOrElse(List.empty)

 // log.debug(s"Scanning for transports in package(s): [${transportsPkg.mkString}].")

  private val pkgs = ingestorsPkg ::: transportsPkg ::: List("hydra")

  private val reflections = new Reflections(pkgs.toSet.toArray)

  override lazy val ingestors = reflections.getSubTypesOf(classOf[Ingestor])
    .asScala.filterNot(c => Modifier.isAbstract(c.getModifiers)).toSeq

  override lazy val transports = reflections.getSubTypesOf(classOf[Transport])
    .asScala.filterNot(c => Modifier.isAbstract(c.getModifiers)).toSeq

}
