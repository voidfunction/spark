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

import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.ListBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.hdi.{InputInformationUpdate, OutputEvent, TableOutputEvent}
import org.apache.spark.scheduler._
import org.apache.spark.ui._

private[ui] class JobGraphTab(parent: SparkUI) extends SparkUITab(parent, "graph") {
  val listener = parent.jobInfoListener
  val conf = parent.conf
  attachPage(new JobGraphPage(this))
}

@DeveloperApi
class JobInfoListener extends SparkListener {
  var nodes = Map[Int, GraphNodeInfo]()
  var allTasksProfiles = LinkedHashMap[Int, LinkedHashMap[Int, ListBuffer[SparkListenerTaskEnd]]]()
  var stageTasksProfiles = LinkedHashMap[Int, LinkedHashMap[Long, SparkListenerTaskEnd]]()
  var stagesProfiles = LinkedHashMap[Int, LinkedHashMap[Int, SparkListenerStageCompleted]]()
  var edges = Seq[GraphEdgeInfo]()
  var startTime: Long = 0
  var endTime: Long = 0
  val timeScale = 301
  var time = 0L

  def populateGraphNodeAndEdge(): Unit = {
    for ((stageId, stage) <- nodes) {
      stage.totalCount = stageTasksProfiles(stageId).size
      val attemptId = stagesProfiles(stageId).size - 1
      stage.time = stagesProfiles(stageId)(attemptId).stageInfo.completionTime.get -
        stagesProfiles(stageId)(0).stageInfo.submissionTime.get
      for ((taskId, task) <- stageTasksProfiles(stageId)) {
        stage.rowCount += task.taskMetrics.inputMetrics.recordsRead
        stage.rowCount += task.taskMetrics.outputMetrics.recordsWritten
        stage.dataRead += task.taskMetrics.inputMetrics.bytesRead
        stage.dataWritten += task.taskMetrics.outputMetrics.bytesWritten
        if (task.taskInfo.failed) {
          stage.failedCount += 1
        }
        else {
          stage.succeededCount += 1
        }
      }
    }
  }

  def populateStagePlaybackSliceStatistics(): Unit = {
    val timeUnit = (Math.ceil((endTime - startTime).toFloat / (timeScale - 1).toFloat)).toInt
    time = endTime - startTime
    for(i <- 0 until timeScale) {
      val currentTime: Long = timeUnit * i + startTime
      for((stageId, stage) <- nodes) {
        val taskCount = stage.totalCount
        var taskCompleted = 0
        var taskFailed = 0
        var taskProgress = 0
        var read = 0L
        var write = 0L
        var time = currentTime - stagesProfiles(stageId)(0).stageInfo.submissionTime.get
        if (time < 0) time = 0
        for( (attemptId, stageAttempt) <- stagesProfiles(stageId)) {
          if ( stageAttempt.stageInfo.submissionTime.isDefined
            && stageAttempt.stageInfo.submissionTime.get < currentTime
            && stageAttempt.stageInfo.completionTime.isDefined
            && stageAttempt.stageInfo.completionTime.get > currentTime
          ) {
            for (task <- allTasksProfiles(stageId)(attemptId)) {
              if (task.taskInfo.finishTime <= currentTime) {
                read += task.taskMetrics.inputMetrics.bytesRead
                write += task.taskMetrics.outputMetrics.bytesWritten
                if(task.taskInfo.failed) taskFailed += 1
                else taskCompleted += 1
              }
              else if (task.taskInfo.launchTime <= currentTime
                && task.taskInfo.finishTime > currentTime
              ) {
                taskProgress += 1
                val percentage: Float = (currentTime - task.taskInfo.launchTime).toFloat /
                  (task.taskInfo.finishTime - task.taskInfo.launchTime).toFloat
                read += (task.taskMetrics.inputMetrics.bytesRead * percentage).toLong
                write += (task.taskMetrics.outputMetrics.bytesWritten * percentage).toLong
              }
            }
          } else if (stageAttempt.stageInfo.completionTime.isDefined
            && stageAttempt.stageInfo.completionTime.get <= currentTime) {
            for (task <- allTasksProfiles(stageId)(attemptId)) {
              read += task.taskMetrics.inputMetrics.bytesRead
              write += task.taskMetrics.outputMetrics.bytesWritten
              if (task.taskInfo.failed) taskFailed += 1
              else taskCompleted += 1
            }
          }
        }
        stage.playBackDataSlice += StagePlayBackDataSlice(
          (taskCompleted * 100 /taskCount).toString,
          taskCompleted * 100 /taskCount,
          taskFailed * 100 / taskCount,
          taskProgress * 100 / taskCount,
          read, write, time)
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    allTasksProfiles(taskEnd.stageId)(taskEnd.stageAttemptId) += taskEnd
    stageTasksProfiles(taskEnd.stageId) += taskEnd.taskInfo.taskId -> taskEnd
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageId = stageSubmitted.stageInfo.stageId
    val attemptId = stageSubmitted.stageInfo.attemptId
    if(!allTasksProfiles.keySet.exists(_ == stageId)) {
      allTasksProfiles += stageId -> LinkedHashMap[Int, ListBuffer[SparkListenerTaskEnd]]()
    }

    if(!allTasksProfiles(stageId).keySet.exists(_ == attemptId)) {
      allTasksProfiles(stageId) +=  attemptId -> ListBuffer[SparkListenerTaskEnd]()
    }

    stageTasksProfiles += stageId -> LinkedHashMap[Long, SparkListenerTaskEnd]()
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    val attemptId = stageCompleted.stageInfo.attemptId
    if (!stagesProfiles.keySet.exists(_ == stageId)) {
      stagesProfiles += stageId -> LinkedHashMap[Int, SparkListenerStageCompleted]()
    }

    stagesProfiles(stageId) +=  attemptId -> stageCompleted
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobStart.stageInfos.foreach(stage =>
      this.nodes += stage.stageId -> new GraphNodeInfo(stage.stageId, stage.name))

    jobStart.stageInfos.foreach(
      stage => stage.parentIds.foreach(id =>
        this.edges = this.edges ++ Seq(GraphEdgeInfo(stage.stageId, id)))
    )
    this.startTime = jobStart.time
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    this.endTime = jobEnd.time
    this.populateGraphNodeAndEdge()
    this.populateStagePlaybackSliceStatistics()
  }
}
