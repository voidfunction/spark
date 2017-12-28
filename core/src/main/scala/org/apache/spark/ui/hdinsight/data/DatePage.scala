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

package org.apache.spark.ui.hdinsight.data

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.ui.{UIUtils, WebUIPage}

private[ui] class DatePage(parent: DataTab) extends WebUIPage("") {
  private val listener = parent.listener
  private def outputPropertyHeader = Seq("Provider", "Mode", "Path")
  private def inputPropertyHeader = Seq("Format", "Path")

  def dataTabInfoNode: Seq[Node] = {
    <script type="text/javascript">
      ApplicationDataInfo =
      {scala.xml.Unparsed(DataUtils.getWrappedInfo(listener.inputs, listener.outputs))}
    </script>
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val sandBoxProperties = "allow-forms allow-modals allow-scripts " +
      "allow-popups allow-same-origin allow-top-navigation"
    val content =
      <div class="container-fluid row" height="0" padding-bottom="100%">
        {dataTabInfoNode}
        <iframe id="datatabiframe" sandbox={sandBoxProperties}
                src="http://localhost:5555"
                style="width:98vw;height:99vh;position:absolute;"
                frameborder="0">
        </iframe>
      </div>
    UIUtils.headerSparkPage("Data", content, parent)
  }
}