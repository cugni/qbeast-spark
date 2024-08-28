package io.qbeast.spark.hudi

import org.apache.spark.SparkConf

/**
 * _VERSION=3.5 ./bin/spark-shell --packages
 * org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.15.0 \ --conf
 * 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \ --conf
 * 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \ --conf
 * 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \ --conf
 * 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' bck-i-search: spark_
 */
class QbeastHudiTest extends QbeastHudiIntegrationTestSpec {

  val hudiSparkConf = new SparkConf()
    .setMaster("local[8]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")

  "Qbeast with Hudi " should "write the data on a new table" in withExtendedSparkAndTmpDir(
    hudiSparkConf) { (spark, tmpDir) =>
    val tableName = "trips_table"
    val basePath = s"file:///${tmpDir}/trips_table"

    val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city")
    val data =
      Seq(
        (
          1695159649087L,
          "334e26e9-8355-45cc-97c6-c31daf0df330",
          "rider-A",
          "driver-K",
          19.10,
          "san_francisco"),
        (
          1695091554788L,
          "e96c4396-3fad-413a-a942-4cb36106d721",
          "rider-C",
          "driver-M",
          27.70,
          "san_francisco"),
        (
          1695046462179L,
          "9909a8b1-2d15-4d3d-8ec9-efc48c536a00",
          "rider-D",
          "driver-L",
          33.90,
          "san_francisco"),
        (
          1695046462179L,
          "9909a8b1-2d15-4d3d-8ec9-efc48c536a00",
          "rider-D",
          "driver-L",
          339.0,
          "san_francisco"),
        (
          1695516137016L,
          "e3cf430c-889d-4015-bc98-59bdce1e530c",
          "rider-F",
          "driver-P",
          34.15,
          "sao_paulo"),
        (
          1695115999911L,
          "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa",
          "rider-J",
          "driver-T",
          17.85,
          "chennai"));

    val inserts = spark.createDataFrame(data).toDF(columns: _*)

    (inserts
      .coalesce(1)
      .write
      .format("hudi")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.clustering.inline", "true")
      .option("hoodie.clustering.inline.max.commits", "2")
      // .option("hoodie.clustering.plan.strategy.class", "")
      .option(
        "hoodie.clustering.execution.strategy.class",
        "io.qbeast.spark.hudi.QbeastHudiExecutionStrategy")
      .option("hoodie.table.name", tableName)
      .mode("overwrite")
      .save(basePath))

  }

}
