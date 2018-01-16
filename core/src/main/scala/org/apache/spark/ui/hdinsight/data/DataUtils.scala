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

import java.net.URLDecoder
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.hadoop.fs.Path

object DataUtils {
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._

  def getWrappedInfo(inputs: Seq[InputInfo], outputs: Seq[OutputInfo]): String = {
    val json =
      "data" ->
        ("inputs" -> inputs.map {
          input => List(input.inputsetid.toString, input.format, input.path)
        }) ~
        ("outputs" -> outputs.map {
          output => List(output.provider, output.mode, output.path)
        })
    compact(render(json))
  }

  // TODO: handle data related actions
  def handleAppDataRequest(req: HttpServletRequest, res: HttpServletResponse): Unit = {
    res.addHeader("Access-Control-Allow-Origin", "*")
    val queries = req.getQueryString.split("&").map(elem => {
      val elems = elem.split("=")
      (URLDecoder.decode(elems(0), "utf-8"), URLDecoder.decode(elems(1), "utf-8"))
    }).toMap
    val filePath = queries.get("url").get
    val format = queries.get("format").getOrElse("text")
    val rowCount = queries.get("rowCount").getOrElse("100").toInt
    val response = LivyClient.getResult(filePath, format, rowCount)

    res.setStatus(200)
    res.setHeader("Content-Type", "application/octet-stream")
    res.setHeader("Content-Disposition", s"""attachment; filename="${new Path(filePath).getName}""")
    res.getWriter.print(response)
    res.getWriter.close()
  }
}

case class InputInfo(inputsetid: Int, format: String, path: String) {
  override def toString: String = s"[$format,$path]"
}

case class OutputInfo(provider: String, mode: String, path: String) {
  override def toString: String = s"[$provider,$mode,$path]"
}