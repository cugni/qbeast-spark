package io.qbeast.spark.hudi

import io.qbeast.core.model._
import io.qbeast.core.transform.HashTransformer
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.spark.delta.writer.RollupDataWriter
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.spark.index.SparkOTreeManager
import org.apache.avro.Schema
import org.apache.hudi.client.clustering.run.strategy.MultipleSparkJobExecutionStrategy
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.model.HoodieFileGroupId
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.table.BulkInsertPartitioner
import org.apache.hudi.table.HoodieTable
import org.apache.hudi.HoodieDatasetBulkInsertHelper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
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
      "Starting clustering with Qbeast for a group, parallelism:" + numOutputGroups + " commit:" + instantTime)
    val newConfig = HoodieWriteConfig.newBuilder
      .withBulkInsertParallelism(numOutputGroups)
      .withProps(getWriteConfig.getProps)
      .build

    newConfig.setValue(
      HoodieStorageConfig.PARQUET_MAX_FILE_SIZE,
      String.valueOf(getWriteConfig.getClusteringTargetFileMaxBytes))

    val partitioner = getQbeastRowPartitionerAsRow(strategyParams, schema)
    val repartitionedRecords = partitioner.repartitionRecords(inputRecords, numOutputGroups)

    HoodieDatasetBulkInsertHelper.bulkInsert(
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
        val tableId = QTableID(table.getConfig.getTableName)
        val transformers =
          Vector(LinearTransformer("ts", LongDataType), HashTransformer("uuid", StringDataType))
        val transformations = transformers.map(t => t.makeTransformation(r => r))
        val rel = Revision.firstRevision(tableId, 10, transformers, transformations)

        val indexStatus = IndexStatus.empty(rel)
        val (qbeastData, tableChanges) = SparkOTreeManager.index(records, indexStatus)

        val rolledUp = RollupDataWriter.rollupDataset(qbeastData, tableChanges)

        rolledUp.repartition(col(QbeastColumns.cubeToRollupColumnName))

      }

      override def arePartitionRecordsSorted = false
    }

}
