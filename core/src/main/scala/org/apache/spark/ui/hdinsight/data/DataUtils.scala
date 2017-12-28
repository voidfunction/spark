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

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

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

  }
}

case class InputInfo(inputsetid: Int, format: String, path: String) {
  override def toString: String = s"[$format,$path]"
}

case class OutputInfo(provider: String, mode: String, path: String) {
  override def toString: String = s"[$provider,$mode,$path]"
}

object JsonExample extends App {
  import org.json4s._
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._

  case class Winner(id: Long, numbers: List[Int])
  case class Lotto(id: Long, winningNumbers: List[Int],
                   winners: List[Winner],
                   drawDate: Option[java.util.Date])

  val winners = List(Winner(23, List(2, 45, 34, 23, 3, 5)), Winner(54, List(52, 3, 12, 11, 18, 22)))
  val lotto = Lotto(5, List(2, 45, 34, 23, 7, 5, 3), winners, None)
}