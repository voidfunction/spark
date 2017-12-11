/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ui.data

import java.util.concurrent.locks.ReentrantLock

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.ui._

// case class OutputEvent(mode: String, schema: StructType,
// options: Map[String, String], provider: String) extends SparkListenerEvent
// case class InputEvent(properties: Map[String, String]) extends SparkListenerEvent

private[ui] class DataTab(parent: SparkUI) extends SparkUITab(parent, "data") {
  val listener = parent.dataListener
  val conf = parent.conf
  attachPage(new DatePage(this))
}

@DeveloperApi
class DataListener extends SparkListener {
  var testInformation = Seq[(String, String)]()

  val otherEventLock: ReentrantLock = new ReentrantLock
  val jobStartEventLock: ReentrantLock = new ReentrantLock

  override def onOtherEvent(event: SparkListenerEvent) {

//    otherEventLock.lock()
  val v = 1
//    otherEventLock.unlock()
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
//    jobStartEventLock.lock()
//    jobStart.stageInfos
    val v = 1
//    testInformation = Seq(("testkey", "testvalue"))
//    jobStartEventLock.unlock()
  }

//  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) {
//
//    val environmentDetails = environmentUpdate.environmentDetails
//      jvmInformation = environmentDetails("JVM Information")
//      sparkProperties = environmentDetails("Spark Properties")
//      systemProperties = environmentDetails("System Properties")
//      classpathEntries = environmentDetails("Classpath Entries")
//  }
}
