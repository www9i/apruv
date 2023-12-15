package com.hs.bdes

import com.esi.dpe.build.ProcessBuilder
import com.esi.dpe.build.ProcessBuilder.DPFInput
import com.esi.dpe.config.ConfigParser
import com.esi.dpe.utils.TestUtils
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import java.io.File

class ConditionDpfConfigTest extends FunSuite with LazyLogging with BeforeAndAfter with BeforeAndAfterAll with SparkTestSession {

  before {
    println("\n***** Starting Test " + new java.util.Date())
  }

  test(testName = "ConditionDpfConfigTest") {
    val confFile = "conf/work_to_cnfz/condition.conf"

    val testDataHome = "src/test/resources/"

    val inputList = List(
      testDataHome + "ConditionDpfConfigTestInput.json"
    )

    val config = ConfigFactory
      .parseFile(new File(confFile))
      .resolve()

    val (dpfConfigs, where, addCols, dropCols) = ConfigParser
      .parseConfig(config, key = "source.mapping")

    val dpfInput = DPFInput(dpfConfigs, where, addCols, dropCols)

    val dfList = dpfConfigs.zip(inputList).map(x => {
      val inputDf = spark.read
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .json(x._2)
      inputDf
    })

    val transformedDF = ProcessBuilder
      .transformInput(spark, config, dpfInput, dfList)
      .drop("data_lake_insert_ts", "data_lake_last_update_ts", "rn")

    // Comparing only primitive columns
    // TODO change output file to ORC/Parquet and compare all columns.
    val cols = transformedDF
      .schema
      .filterNot(x => List("struct", "array").exists(y => x.dataType.typeName.contains(y)))
      .map(_.name)

    val actualDF = transformedDF
      .selectExpr(cols: _*)

    val expectedDF = spark.read
      .option("delimiter", "|")
      .option("header", "true")
      .csv("src/test/resources/ConditionDpfConfigTestExpected.csv")

    val (diffCount1, diffCount2) = TestUtils.compareResultWithExpected(actualDF, expectedDF)
    assert(diffCount1 === 0 && diffCount2 === 0)
  }

    after {
      println("***** Completed Test " + new java.util.Date() + "\n")
      spark.stop()
    }
}
