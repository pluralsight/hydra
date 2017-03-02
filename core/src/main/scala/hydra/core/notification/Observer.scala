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

package hydra.core.notification

import akka.actor.ActorSystem

/**
 * Created by alexsilva on 2/18/16.
 */
trait HydraEvent[S] {
  def source: S
}

trait Observer[S] {

  def receiveUpdate(evt: HydraEvent[S])(implicit system: ActorSystem)
}

trait Subject[S] {

  private var observers: List[Observer[S]] = Nil

  def addObserver(observer: Observer[S]) = observers = observer :: observers

  def notifyObservers(evt: HydraEvent[S])(implicit system: ActorSystem) = observers.foreach(_.receiveUpdate(evt))
}
