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

package org.apache.spark.ui.hdinsight.log

import javax.servlet.http.HttpServletRequest
import scala.xml.Node

import org.apache.spark.ui.{UIUtils, WebUIPage}

private[ui] class LogPage(parent: LogTab) extends WebUIPage("") {
  def render(request: HttpServletRequest): Seq[Node] = {
    val logTabIFrameUrl = parent.conf.getOption("spark.hdinsight.logFrame").getOrElse("")
    val sandBoxProperties = "allow-forms allow-modals allow-scripts " +
      "allow-popups allow-same-origin allow-top-navigation"
    val content =
      <div class="container-fluid row" height="0" padding-bottom="100%">
        <iframe id="logtabiframe" sandbox={sandBoxProperties}
                src={logTabIFrameUrl}
                style="width:98vw;height:99vh;position:absolute;"
                frameborder="0">
        </iframe>
      </div>
    UIUtils.headerSparkPage("Log", content, parent)
  }
}