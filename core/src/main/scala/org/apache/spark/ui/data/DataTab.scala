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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.hdi.{InputInformationUpdate, OutputEvent, TableOutputEvent}
import org.apache.spark.scheduler._
import org.apache.spark.ui._

private[ui] class DataTab(parent: SparkUI) extends SparkUITab(parent, "data") {
  val listener = parent.dataListener
  val conf = parent.conf
  attachPage(new DatePage(this))
}

@DeveloperApi
class DataListener extends SparkListener {
  var outputInformation = Seq[(String, String, String)]()
  var inputInformation = Seq[(String, String)]()
  private val pattern = "^InMemoryFileIndex\\[(.*)\\]$".r

  override def onOtherEvent(event: SparkListenerEvent) {
      event match {
        case e: OutputEvent =>
          val path = e.provider match {
            case "jdbc" =>
              val dbtable = e.options.getOrElse("dbtable", "None")
              e.options.get("url").map((url) => {
                val properties = new java.util.Properties()
                for (u <- url.split(";")) {
                  val v = u.split("=")
                  if (v.length == 2) {
                    properties.put(v(0), v(1))
                  }
                }
                s"${properties.getProperty("database")} -> $dbtable"
              }).getOrElse("None")

            case _ => e.options.getOrElse("path", "None")
          }
          outputInformation = outputInformation ++
            Seq((e.provider, e.mode, path))
        case e: InputInformationUpdate =>
          e.locations match {
            case pattern(locs) =>
              val inputs = locs.split(",").toSeq.map((e.format, _))
              inputInformation = inputInformation ++ inputs
            case _ =>
          }
        case e: TableOutputEvent =>
          outputInformation = outputInformation ++
            Seq(("table", "Append", String.format("%s->%s", e.database, e.name)))
        case _ =>
      }
  }
}
