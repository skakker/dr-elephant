/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis.Severity
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ExecutorStageSummary, StageData}
import com.linkedin.drelephant.analysis._
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.util.MemoryFormatUtils

import scala.collection.JavaConverters


/**
  * A heuristic based on memory spilled.
  *
  */
class ExecutorStorageSpillHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import ExecutorStorageSpillHeuristic._
  import JavaConverters._

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)
    var resultDetails = Seq(
      new HeuristicResultDetails("Total memory spilled", MemoryFormatUtils.bytesToString(evaluator.totalMemorySpilled))
    )

    if(evaluator.severity != Severity.NONE){
      resultDetails :+ new HeuristicResultDetails("Note", "Your memory is being spilled. Kindly look into it.")
      if(evaluator.sparkExecutorCores >=4 && evaluator.sparkExecutorMemory >= MemoryFormatUtils.stringToBytes("10GB")) {
        resultDetails :+ new HeuristicResultDetails("Recommendation", "You can try decreasing the number of cores to reduce the number of concurrently running tasks.")
      }
    }

    val result = new HeuristicResult(
      heuristicConfigurationData.getClassName,
      heuristicConfigurationData.getHeuristicName,
      evaluator.severity,
      0,
      resultDetails.asJava
    )
    result
  }
}

object ExecutorStorageSpillHeuristic {
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_EXECUTOR_CORES = "spark.executor.cores"

  class Evaluator(memoryFractionHeuristic: ExecutorStorageSpillHeuristic, data: SparkApplicationData) {
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties
    lazy val stageDatas: Seq[StageData] = data.stageDatas
    val ratioMemoryCores: Double = sparkExecutorMemory.toDouble / sparkExecutorCores.toDouble
    lazy val (severity, totalMemorySpilled : Long) = getExecutionSpillSeverity()
    def getExecutionSpillSeverity(): (Severity, Long) = {
      var bytesSpilled : Long = 0
      var executionSpillSeverity = Severity.NONE
      stageDatas.foreach(stageData => {
        val executorStageList: collection.Map[String, ExecutorStageSummary] = stageData.executorSummary.getOrElse(Map.empty)
        val maxMemorySpilled: Long = executorStageList.values.map(_.memoryBytesSpilled).max
        val meanMemorySpilled = executorStageList.values.map(_.memoryBytesSpilled).sum / executorStageList.values.size
        val ratioMemoryBytesSpilled: Double = executorStageList.values.count(_.memoryBytesSpilled > 0).toDouble / executorStageList.values.size.toDouble
        bytesSpilled += executorStageList.values.count(_.memoryBytesSpilled > 0).toLong
        val severityExecutionSpillStage: Severity = {
          if (ratioMemoryBytesSpilled != 0) {
            if (ratioMemoryBytesSpilled < 0.2 && maxMemorySpilled < 0.05 * ratioMemoryCores) {
              Severity.LOW
            }
            else if (ratioMemoryBytesSpilled < 0.2 && meanMemorySpilled < 0.05 * ratioMemoryCores) {
              Severity.MODERATE
            }

            else if (ratioMemoryBytesSpilled >= 0.2 && meanMemorySpilled < 0.05 * ratioMemoryCores) {
              Severity.SEVERE
            }
            else if (ratioMemoryBytesSpilled >= 0.2 && meanMemorySpilled >= 0.05 * ratioMemoryCores) {
              Severity.CRITICAL
            }
          }
          Severity.NONE
        }
        executionSpillSeverity = Severity.max(executionSpillSeverity, severityExecutionSpillStage)
      })
      (executionSpillSeverity, bytesSpilled)
    }

    val sparkExecutorMemory: Long = (appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY).map(MemoryFormatUtils.stringToBytes)).getOrElse(0L)
    val sparkExecutorCores: Int = (appConfigurationProperties.get(SPARK_EXECUTOR_CORES).map(_.toInt)).getOrElse(0)
  }
}

