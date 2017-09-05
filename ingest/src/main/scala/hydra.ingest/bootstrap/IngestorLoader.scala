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

import hydra.core.ingest.Ingestor
import org.reflections.Reflections

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 1/12/16.
  */
trait IngestorLoader {

  def ingestors: Seq[Class[_ <: Ingestor]]
}

class ClasspathIngestorLoader(pkgs: Seq[String]) extends IngestorLoader {

  require(pkgs.size > 0, "At least one package is required.")

  private val reflections = new Reflections(pkgs.toArray)

  override lazy val ingestors = reflections.getSubTypesOf(classOf[Ingestor])
    .asScala.filterNot(c => Modifier.isAbstract(c.getModifiers)).toSeq
}
