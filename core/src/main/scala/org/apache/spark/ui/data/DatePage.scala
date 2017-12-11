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

  def render(request: HttpServletRequest): Seq[Node] = {
    val testInformationTable = UIUtils.listingTable(
      propertyHeader, propertyRow, listener.testInformation, fixedWidth = true)
//    val sparkPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
//      Utils.redact(parent.conf, listener.sparkProperties), fixedWidth = true)
//
//    val systemPropertiesTable = UIUtils.listingTable(
//      propertyHeader, propertyRow, listener.systemProperties, fixedWidth = true)
//    val classpathEntriesTable = UIUtils.listingTable(
//      classPathHeaders, classPathRow, listener.classpathEntries, fixedWidth = true)
//    UIUtils.listingTable("test", )
    val content =
      <span>
        <h4>Input Information</h4> {testInformationTable}
      </span>

    UIUtils.headerSparkPage("Data", content, parent)
  }

  private def propertyHeader = Seq("Name", "Value")
  private def classPathHeaders = Seq("Resource", "Source")
  private def jvmRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def classPathRow(data: (String, String)) = <tr><td>{data._1}</td><td>{data._2}</td></tr>
}