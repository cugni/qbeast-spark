/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.hudi

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.qbeast.core.model.IndexManager
import io.qbeast.spark.index.SparkOTreeManager
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

/**
 * This class contains all function that you should use to test qbeast over spark. You can use it
 * like:
 * {{{
 *  "my class" should "write correctly the data" in withSparkAndTmpDir {
 *   (spark,tmpDir) =>{
 *       List(("hola",1)).toDF.write.parquet(tmpDir)
 *
 *  }
 * }}}
 */
trait QbeastHudiIntegrationTestSpec extends AnyFlatSpec with Matchers with DatasetComparer {

  // Spark Configuration
  // Including Session Extensions and Catalog
  def sparkConfWithSqlAndCatalog: SparkConf = new SparkConf()
    .setMaster("local[8]")



  /**
   * This function is used to create a spark session with the given configuration.
   * @param sparkConf
   *   the configuration
   * @param testCode
   *   the code to run within the spark session
   * @tparam T
   * @return
   */
  def withExtendedSpark[T](sparkConf: SparkConf = new SparkConf())(
      testCode: SparkSession => T): T = {
    val spark = SparkSession
      .builder()
      .appName("QbeastDataSource")
      .config(sparkConf)
      .getOrCreate()
    spark.sparkContext.setLogLevel(Level.WARN.toString)
    try {
      testCode(spark)
    } finally {
      spark.close()
    }
  }

  def withExtendedSparkAndTmpDir[T](sparkConf: SparkConf = new SparkConf())(
      testCode: (SparkSession, String) => T): T = {
    withTmpDir(tmpDir => withExtendedSpark(sparkConf)(spark => testCode(spark, tmpDir)))
  }

  /**
   * This function is used to create a spark session
   * @param testCode
   *   the code to test within the spark session
   * @tparam T
   * @return
   */
  def withSpark[T](testCode: SparkSession => T): T = {
    withExtendedSpark(sparkConfWithSqlAndCatalog)(testCode)
  }

  /**
   * Runs code with a Temporary Directory. After execution, the content of the directory is
   * deleted.
   * @param testCode
   * @tparam T
   * @return
   */
  def withTmpDir[T](testCode: String => T): T = {
    val directory = Files.createTempDirectory("qb-testing")
    try {
      testCode(directory.toString)
    } finally {
      import scala.reflect.io.Directory
      val d = new Directory(directory.toFile)
      d.deleteRecursively()
    }
  }

  def withSparkAndTmpDir[T](testCode: (SparkSession, String) => T): T =
    withTmpDir(tmpDir => withSpark(spark => testCode(spark, tmpDir)))

  /**
   * Runs code with Warehouse/Catalog extensions
   * @param testCode
   *   the code to reproduce
   * @tparam T
   * @return
   */
  def withQbeastContextSparkAndTmpWarehouse[T](testCode: (SparkSession, String) => T): T =
    withTmpDir(tmpDir =>
      withExtendedSpark(
        sparkConfWithSqlAndCatalog
          .set("spark.sql.warehouse.dir", tmpDir))(spark => testCode(spark, tmpDir)))

  /**
   * Runs code with OTreeAlgorithm configuration
   * @param code
   * @tparam T
   * @return
   */
  def withOTreeAlgorithm[T](code: IndexManager[DataFrame] => T): T = {
    code(SparkOTreeManager)
  }

}
