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

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.ui.{UIUtils, WebUIPage}

private[ui] class DatePage(parent: DataTab) extends WebUIPage("") {
  private val listener = parent.listener
  private def outputPropertyHeader = Seq("Provider", "Mode", "Path")
  private def inputPropertyHeader = Seq("Format", "Path")

  private def planVisualizationResources: Seq[Node] = {
    // scalastyle:off
      <link rel="stylesheet" href={UIUtils.prependBaseUri("/static/sql/spark-sql-viz.css")} type="text/css"/>
      <script src={UIUtils.prependBaseUri("/static/d3.min.js")}></script>
      <script src={UIUtils.prependBaseUri("/static/dagre-d3.min.js")}></script>
      <script src={UIUtils.prependBaseUri("/static/graphlib-dot.min.js")}></script>
      <script src={UIUtils.prependBaseUri("/static/sql/spark-sql-viz.js")}></script>
    // scalastyle:on
  }

  def dataTabInfoNode: Seq[Node] = {
    <p>test</p>
      <script>
        {"hdiinfo = " + listener.inputInformation.toString()}
      </script>
      <script src={UIUtils.prependBaseUri("/static/data/jobdata.js")}></script>
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val outputTable = UIUtils.listingTable(
      outputPropertyHeader,
      outputPropertyRow,

      listener.outputInformation,
      fixedWidth = true)

    val inputTable = UIUtils.listingTable(
      inputPropertyHeader,
      inputPropertyRow,
      listener.inputInformation,
      fixedWidth = true)
//    val content =
//      <span>
//        <h4>Input Information</h4> {inputTable}
//        <hr/>
//        <h4>Output Information</h4> {outputTable}
//      </span>
    val sandBoxProperties = "allow-forms allow-scripts allow-popups allow-same-origin allow-top-navigation"
    val content =
      <div class="container-fluid row" height="0" padding-bottom="100%">
        {dataTabInfoNode}
        <iframe sandbox={sandBoxProperties}
                src="http://localhost:5555"
                style="width:98vw;height:99vh;position:absolute;"
                frameborder="0">
        </iframe>
      </div>
    UIUtils.headerSparkPage("Data", content, parent)
  }

  private def inputPropertyRow(values: (String, String)) = {
    <tr><td>{values._1}</td><td>{values._2}</td></tr>
  }

  private def outputPropertyRow(values : (String, String, String)) =
            <tr><td>{values._1}</td><td>{values._2}</td><td>{values._3}</td></tr>
}