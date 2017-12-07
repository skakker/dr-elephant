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
import com.linkedin.drelephant.spark.fetchers.statusapiv1._
import com.linkedin.drelephant.analysis._
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import scala.collection.JavaConverters

/**
  * A heuristic based on GC time and CPU run time
  *
  */
class GcCpuTimeHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import GcCpuTimeHeuristic._
  import JavaConverters._

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)
    var resultDetails = Seq(
      new HeuristicResultDetails("GC time to Executor Run time ratio", evaluator.ratio.toString),
      new HeuristicResultDetails("GC total time", evaluator.jvmTime.toString),
      new HeuristicResultDetails("Executor Run time", evaluator.executorRunTimeTotal.toString)
    )

    //adding recommendations to the result, severityTimeA corresponds to the ascending severity calculation
    if (evaluator.severityTimeA.getValue > Severity.LOW.getValue) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Note", "The ratio of JVM GC Time and executor Time is above normal, we recommend to increase the executor memory")
    }
    //severityTimeD corresponds to the descending severity calculation
    if (evaluator.severityTimeD.getValue > Severity.LOW.getValue) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Note", "The ratio of JVM GC Time and executor Time is below normal, we recommend to decrease the executor memory")
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

object GcCpuTimeHeuristic {
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_EXECUTOR_CORES = "spark.executor.cores"

  /** The ascending severity thresholds for the ratio of JVM GC Time and executor Run Time (checking whether ratio is above normal)
    * These thresholds are experimental and are likely to change */
  val DEFAULT_GC_SEVERITY_A_THRESHOLDS =
    SeverityThresholds(low = 0.08D, moderate = 0.1D, severe = 0.15D, critical = 0.2D, ascending = true)

  /** The descending severity thresholds for the ratio of JVM GC Time and executor Run Time (checking whether ratio is below normal)
    * These thresholds are experimental and are likely to change */
  val DEFAULT_GC_SEVERITY_D_THRESHOLDS =
    SeverityThresholds(low = 0.05D, moderate = 0.04D, severe = 0.03D, critical = 0.01D, ascending = false)

  class Evaluator(memoryFractionHeuristic: GcCpuTimeHeuristic, data: SparkApplicationData) {
    lazy val executorSummaries: Seq[ExecutorSummary] = data.executorSummaries
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties
    var (jvmTime, executorRunTimeTotal) = getTimeValues(executorSummaries)

    var ratio: Double = jvmTime.toDouble / executorRunTimeTotal.toDouble

    lazy val severityTimeA: Severity = DEFAULT_GC_SEVERITY_A_THRESHOLDS.severityOf(ratio)
    lazy val severityTimeD: Severity = DEFAULT_GC_SEVERITY_D_THRESHOLDS.severityOf(ratio)
    lazy val severity : Severity = Severity.max(severityTimeA, severityTimeD)

    /**
      * returns the total JVM GC Time and total executor Run Time across all stages
      * @param executorSummaries
      * @return
      */
    private def getTimeValues(executorSummaries: Seq[ExecutorSummary]): (Long, Long) = {
      var jvmGcTimeTotal: Long = 0
      var executorRunTimeTotal: Long = 0
      executorSummaries.foreach(executorSummary => {
        jvmGcTimeTotal+=executorSummary.totalGCTime
        executorRunTimeTotal+=executorSummary.totalDuration
      })
      (jvmGcTimeTotal, executorRunTimeTotal)
    }
  }
}

