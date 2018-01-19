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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{BasicCredentialsProvider, HttpClients, SystemDefaultCredentialsProvider}
import org.apache.http.util.EntityUtils
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.util.ShutdownHookManager

class LivyClient(val clusterConnectString: String,
                val userName: String,
                val password: String) {

  implicit val formats = DefaultFormats

  private lazy val sessionId: Option[Int] = newSession("spark")

  // we should have a unique id to handling multi thread issue
  private lazy val sessionQueryId: AtomicInteger = new AtomicInteger(0)

  private lazy val credentialProvider = new BasicCredentialsProvider()
  credentialProvider.setCredentials(AuthScope.ANY,
      new UsernamePasswordCredentials(userName, password))

  private lazy val httpClient = HttpClients.custom()
                          .setDefaultCredentialsProvider(credentialProvider)
                          .build

  def getQueryCode(format: String, path: String, rowCount: Int = 100): String = {
    val queryId = sessionQueryId.getAndIncrement()
    s"""
        val df${queryId} = spark.read.format("$format").load("$path")
        val result${queryId} = df${queryId}.take($rowCount).map(_.toString).toList

        import org.json4s.JsonDSL._
        import org.json4s.jackson.JsonMethods._

        val json${queryId} = (("schema" -> df${queryId}.schema.json) ~
                    ("format" -> "$format") ~
                    ("maxLine" -> $rowCount) ~
                    ("result" -> result${queryId}))
        println(compact(render(json${queryId})))
        """
  }

  def getOrCreateSession(sessionType: String = "spark"): Option[Int] =
            sessionId.orElse(newSession(sessionType))

  private def getSessionState(sessionId: Int): Boolean = {
    val postUrl = new HttpGet(s"$clusterConnectString/livy/sessions/$sessionId/state")
    val response = httpClient.execute(postUrl)
    val responseString = EntityUtils.toString(response.getEntity)
    val responseObj = parse(responseString)
    val state = (responseObj \ "state").extract[String]
    // scalastyle:off
    println(s"status of session $sessionId: $state")
    // scalastyle:on
    state.equals("idle")
  }

  private def newSession(sessionType: String): Option[Int] = {
    val data = s"""{"kind": "$sessionType"}"""
    val postUrl = new HttpPost(s"$clusterConnectString/livy/sessions")
    postUrl.setHeader("Content-Type", "application/json")
    postUrl.setEntity(new StringEntity(data))
    val response = httpClient.execute(postUrl)
    val responseString = EntityUtils.toString(response.getEntity)
    val responseObj = parse(responseString)
    Some((responseObj \ "id").extract[Int])
  }

  private def execution(sessionId: Int, format: String, path: String, rowCount: Int = 100): Int = {
    val url = s"$clusterConnectString/livy/sessions/$sessionId/statements"
    val postUrl = new HttpPost(url)
    postUrl.setHeader("Content-Type", "application/json")
    val code = getQueryCode(format, path)
    val jsonCode = compact(render("code" -> code))
    postUrl.setEntity(new StringEntity(jsonCode))
    val response = httpClient.execute(postUrl)
    val responseString = EntityUtils.toString(response.getEntity)
    val responseObj = parse(responseString)
    (responseObj \ "id").extract[Int]
  }

  private def getStatementOutput(sessionId: Int, statementId: Int): JValue = {
    val url = s"$clusterConnectString/livy/sessions/$sessionId/statements/$statementId"
    val getUrl = new HttpGet(url)
    getUrl.setHeader("Content-Type", "application/json")
    var status = "running"
    while(status.equals("running")) {
      val response = httpClient.execute(getUrl)
      val responseEntity = EntityUtils.toString(response.getEntity)
      val responseObj = parse(responseEntity)
      status = (responseObj \ "state").extract[String]
      if (status.equals("available")) {
        return responseObj \ "output"
      }
      Thread.sleep(1500)
    }
    ""
  }

  def getOutput(filePath: String, format: String): JValue = {
    val sessionId = getOrCreateSession().get
    val statementId = execution(sessionId, format, filePath)
    val res = getStatementOutput(sessionId, statementId)

    val resultString = (res \ "data" \ "text/plain").extract[String]
    parse(resultString) \ "result"
  }

  def waitSessionState(sessionId: Int): Unit = {
    while (!getSessionState(sessionId)) {
      Thread.sleep(100)
    }
  }

  // try to close the session
  def closeSession: Option[Boolean] = {
    sessionId.map(id => {
      val url = s"$clusterConnectString/livy/sessions/$id"
      val deleteUrl = new HttpDelete(url)
      val response = httpClient.execute(deleteUrl)
      val responseEntity = EntityUtils.toString(response.getEntity)
      val responseObj = parse(responseEntity)
      (responseObj \ "msg").extract[String].equals("deleted")
    })
  }
}

object LivyClient {
  private val conf = HistoryServer.getConf
  private val clusterConnectionString = conf.get("spark.hdinsight.clusterUrl")
  private val userName = conf.get("spark.hdinsight.clusterUserName")
  private val password = conf.get("spark.hdinsight.clusterPassword")

  // scalastyle:off
  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = client.closeSession
  })
  // scalastyle:on

  lazy val client = new LivyClient(clusterConnectionString,
    userName,
    password)

  def getResult(filePath: String, format: String, rowCount: Int): String = {
    implicit val formats = DefaultFormats
    // scalastyle:off

    client.sessionId.map(sessionId => {
      client.waitSessionState(sessionId)
      println(s"current sesssion id: $sessionId")
      val statementId = client.execution(sessionId, format, filePath, rowCount)
      val res = client.getStatementOutput(sessionId, statementId)
      (res \ "data" \ "text/plain").extract[String]
    }).getOrElse("")
  }
}
