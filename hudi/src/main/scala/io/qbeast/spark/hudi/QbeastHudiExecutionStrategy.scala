package io.qbeast.spark.hudi

import org.apache.avro.Schema
import org.apache.hudi.HoodieDatasetBulkInsertHelper
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.client.clustering.run.strategy.MultipleSparkJobExecutionStrategy
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.model.{HoodieFileGroupId, HoodieRecord}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.table.{BulkInsertPartitioner, HoodieTable}
import org.apache.spark.sql.{Dataset, Row}
import org.slf4j.LoggerFactory

import java.util

class QbeastHudiExecutionStrategy[T](
    table: HoodieTable[_, _, _, _],
    engineContext: HoodieEngineContext,
    writeConfig: HoodieWriteConfig)
    extends MultipleSparkJobExecutionStrategy[T](table, engineContext, writeConfig) {

  private val LOG = LoggerFactory.getLogger(classOf[QbeastHudiExecutionStrategy[_]])

  override def performClusteringWithRecordsAsRow(
      inputRecords: Dataset[Row],
      numOutputGroups: Int,
      instantTime: String,
      strategyParams: util.Map[String, String],
      schema: Schema,
      fileGroupIdList: util.List[HoodieFileGroupId],
      shouldPreserveHoodieMetadata: Boolean,
      extraMetadata: util.Map[String, String]): HoodieData[WriteStatus] = {
    LOG.info(
      "Starting clustering for a group, parallelism:" + numOutputGroups + " commit:" + instantTime)
    val newConfig = HoodieWriteConfig.newBuilder
      .withBulkInsertParallelism(numOutputGroups)
      .withProps(getWriteConfig.getProps)
      .build

    newConfig.setValue(
      HoodieStorageConfig.PARQUET_MAX_FILE_SIZE,
      String.valueOf(getWriteConfig.getClusteringTargetFileMaxBytes))

    val partitioner = getQbeastRowPartitionerAsRow(strategyParams, schema)
    val repartitionedRecords = partitioner.repartitionRecords(inputRecords, numOutputGroups)

    return HoodieDatasetBulkInsertHelper.bulkInsert(
      repartitionedRecords,
      instantTime,
      getHoodieTable,
      newConfig,
      partitioner.arePartitionRecordsSorted,
      shouldPreserveHoodieMetadata)
  }

  override def performClusteringWithRecordsRDD(
      inputRecords: HoodieData[HoodieRecord[T]],
      numOutputGroups: Int,
      instantTime: String,
      strategyParams: util.Map[String, String],
      schema: Schema,
      fileGroupIdList: util.List[HoodieFileGroupId],
      shouldPreserveHoodieMetadata: Boolean,
      extraMetadata: util.Map[String, String]): HoodieData[WriteStatus] = ???


  private def getQbeastRowPartitionerAsRow(
      strategyParams: util.Map[String, String],
      schema: Schema): BulkInsertPartitioner[Dataset[Row]] =
    new BulkInsertPartitioner[Dataset[Row]]() {

      override def repartitionRecords(
          records: Dataset[Row],
          outputPartitions: Int): Dataset[Row] = {

        records.repartition(outputPartitions)
      }

      override def arePartitionRecordsSorted = false
    }

}
