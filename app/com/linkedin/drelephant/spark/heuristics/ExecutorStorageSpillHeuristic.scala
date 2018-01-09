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
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ExecutorStageSummary, ExecutorSummary, StageData}
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

  val spillFractionOfExecutorsThreshold: Double =
    if(heuristicConfigurationData.getParamMap.get(SPILL_FRACTION_OF_EXECUTORS_THRESHOLD_KEY) == null) DEFAULT_SPILL_FRACTION_OF_EXECUTORS_THRESHOLD
    else heuristicConfigurationData.getParamMap.get(SPILL_FRACTION_OF_EXECUTORS_THRESHOLD_KEY).toDouble

  val spillMaxMemoryThreshold: Double =
    if(heuristicConfigurationData.getParamMap.get(SPILL_MAX_MEMORY_THRESHOLD_KEY) == null) DEFAULT_SPILL_MAX_MEMORY_THRESHOLD
    else heuristicConfigurationData.getParamMap.get(SPILL_MAX_MEMORY_THRESHOLD_KEY).toDouble


  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)
    var resultDetails = Seq(
      new HeuristicResultDetails("Total memory spilled", MemoryFormatUtils.bytesToString(evaluator.totalMemorySpilled)),
      new HeuristicResultDetails("Max memory spilled", MemoryFormatUtils.bytesToString(evaluator.maxMemorySpilled)),
      new HeuristicResultDetails("Mean memory spilled", MemoryFormatUtils.bytesToString(evaluator.meanMemorySpilled)),
      new HeuristicResultDetails("Fraction of executors having non zero bytes spilled", evaluator.fractionOfExecutorsHavingBytesSpilled.toString)
    )

    if(evaluator.severity != Severity.NONE){
      resultDetails :+ new HeuristicResultDetails("Note", "Your execution memory is being spilled. Kindly look into it.")
      if(evaluator.sparkExecutorCores >=4 && evaluator.sparkExecutorMemory >= MemoryFormatUtils.stringToBytes("10GB")) {
        resultDetails :+ new HeuristicResultDetails("Recommendation", "You can try decreasing the number of cores to reduce the number of concurrently running tasks.")
      } else if (evaluator.sparkExecutorMemory >= MemoryFormatUtils.stringToBytes("10GB")) {
        resultDetails :+ new HeuristicResultDetails("Recommendation", "You can try increasing the executor memory to reduce spill.")
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
  val SPILL_FRACTION_OF_EXECUTORS_THRESHOLD_KEY = "spill_fraction_of_executors_threshold"
  val SPILL_MAX_MEMORY_THRESHOLD_KEY = "spill_max_memory_threshold"
  val DEFAULT_SPILL_FRACTION_OF_EXECUTORS_THRESHOLD : Double = 0.2
  val DEFAULT_SPILL_MAX_MEMORY_THRESHOLD : Double = 0.05

  class Evaluator(executorStorageSpillHeuristic: ExecutorStorageSpillHeuristic, data: SparkApplicationData) {
    lazy val executorSummaries: Seq[ExecutorSummary] = data.executorSummaries
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties
    val memoryPerCore: Long = (sparkExecutorMemory / sparkExecutorCores)
    val maxMemorySpilled: Long = executorSummaries.map(_.totalMemoryBytesSpilled).max
    val meanMemorySpilled = executorSummaries.map(_.totalMemoryBytesSpilled).sum / executorSummaries.size
    val totalMemorySpilled = executorSummaries.map(_.totalMemoryBytesSpilled).sum
    val fractionOfExecutorsHavingBytesSpilled: Double = executorSummaries.count(_.totalMemoryBytesSpilled > 0).toDouble / executorSummaries.size.toDouble
    val severity: Severity = {
      if (fractionOfExecutorsHavingBytesSpilled != 0) {
        if (fractionOfExecutorsHavingBytesSpilled < executorStorageSpillHeuristic.spillFractionOfExecutorsThreshold
          && maxMemorySpilled < executorStorageSpillHeuristic.spillMaxMemoryThreshold * memoryPerCore) {
          Severity.LOW
        }
        else if (fractionOfExecutorsHavingBytesSpilled < executorStorageSpillHeuristic.spillFractionOfExecutorsThreshold
          && meanMemorySpilled < executorStorageSpillHeuristic.spillMaxMemoryThreshold * memoryPerCore) {
          Severity.MODERATE
        }

        else if (fractionOfExecutorsHavingBytesSpilled >= executorStorageSpillHeuristic.spillFractionOfExecutorsThreshold
          && meanMemorySpilled < executorStorageSpillHeuristic.spillMaxMemoryThreshold * memoryPerCore) {
          Severity.SEVERE
        }
        else if (fractionOfExecutorsHavingBytesSpilled >= executorStorageSpillHeuristic.spillFractionOfExecutorsThreshold
          && meanMemorySpilled >= executorStorageSpillHeuristic.spillMaxMemoryThreshold * memoryPerCore) {
          Severity.CRITICAL
        } else Severity.NONE
      }
      else Severity.NONE
    }

    lazy val sparkExecutorMemory: Long = (appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY).map(MemoryFormatUtils.stringToBytes)).getOrElse(0)
    lazy val sparkExecutorCores: Int = (appConfigurationProperties.get(SPARK_EXECUTOR_CORES).map(_.toInt)).getOrElse(0)
  }
}

