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

import com.linkedin.drelephant.analysis._
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.spark.fetchers.statusapiv1.ExecutorSummary
import scala.collection.JavaConverters


/**
  * A heuristic based on peak unified memory for the spark executors
  *
  * This heuristic reports the fraction of memory used/ memory allocated for execution and if the fraction can be reduced. Also, it checks for the skew in peak unified memory and reports if the skew is too much.
  */


class UnifiedMemoryHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import UnifiedMemoryHeuristic._
  import JavaConverters._

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)

    var resultDetails = Seq(
      new HeuristicResultDetails("Max peak unified memory", evaluator.maxUnifiedMemory.toString),
      new HeuristicResultDetails("Mean peak unified memory", evaluator.meanUnifiedMemory.toString)
    )

    if (evaluator.severityPeak.getValue > Severity.LOW.getValue) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Note", "The value of peak unified memory is very low, we recommend to increase the value of spark.memory.fraction")
    }
    if (evaluator.severitySkew.getValue > Severity.LOW.getValue) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Note", "There is a imbalance in the amount of work (and data) for tasks, please see into this so that in can be more balanced")
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

object UnifiedMemoryHeuristic {

  val JVM_USED_MEMORY = "jvmUsedMemory"

  class Evaluator(memoryFractionHeuristic: UnifiedMemoryHeuristic, data: SparkApplicationData) {
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties

    lazy val executorSummaries: Seq[ExecutorSummary] = data.executorSummaries
    val executorList : Seq[ExecutorSummary] = executorSummaries.patch(executorSummaries.indexWhere(_.id.equals("driver")), Nil, 1)

    val maxMemory: Long = executorList.head.maxMemory

    val DEFAULT_PEAK_UNIFIED_MEMORY_THRESHOLDS =
      SeverityThresholds(low = 0.7 * maxMemory, moderate = 0.6 * maxMemory, severe = 0.4 * maxMemory, critical = 0.2 * maxMemory, ascending = false)

    val DEFAULT_UNIFIED_MEMORY_SKEW_THRESHOLDS =
      SeverityThresholds(low = 1.5 * meanUnifiedMemory, moderate = 2 * meanUnifiedMemory, severe = 4 * meanUnifiedMemory, critical = 8 * meanUnifiedMemory, ascending = true)

    def getPeakUnifiedMemoryExecutorSeverity(executorSummary: ExecutorSummary): Severity = {
      var jvmPeakUnifiedMemory: Long = executorSummary.peakUnifiedMemory.getOrElse(JVM_USED_MEMORY, 0)
      return DEFAULT_PEAK_UNIFIED_MEMORY_THRESHOLDS.severityOf(jvmPeakUnifiedMemory)
    }

    lazy val meanUnifiedMemory: Long = (executorList.map {
      _.peakUnifiedMemory.getOrElse(JVM_USED_MEMORY, 0).asInstanceOf[Number].longValue
    }.sum) / executorList.size
    lazy val maxUnifiedMemory: Long = (executorList.map {
      _.peakUnifiedMemory.get(JVM_USED_MEMORY)
    }.max).getOrElse(0)
    val severitySkew = DEFAULT_UNIFIED_MEMORY_SKEW_THRESHOLDS.severityOf(maxUnifiedMemory)

    lazy val severityPeak: Severity = {
      var severityPeakUnifiedMemoryVariable: Severity = Severity.NONE
      for (executorSummary <- executorList) {
        var peakUnifiedMemoryExecutorSeverity: Severity = getPeakUnifiedMemoryExecutorSeverity(executorSummary)
        if (peakUnifiedMemoryExecutorSeverity.getValue > severityPeakUnifiedMemoryVariable.getValue) {
          severityPeakUnifiedMemoryVariable = peakUnifiedMemoryExecutorSeverity
        }
      }
      severityPeakUnifiedMemoryVariable
    }
    lazy val severity: Severity = Severity.max(severityPeak, severitySkew)
  }

}