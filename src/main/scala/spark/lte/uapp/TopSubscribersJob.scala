// This job computes the top 10 subscribers by download bytes, upload bytes and sum(down + up) per hour.

package spark.lte.uapp

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, dense_rank, collect_set}
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.storage.StorageLevel

object TopSubscribersJob {
  private val logger = LoggerFactory.getLogger(getClass)
  private val downLoadColName = Constants.downLoadColName
  private val upLoadColName = Constants.upLoadColName
  private val totalBytesColName = Constants.totalBytesColName
  private val inputTableName = Constants.inputTableName
  private val inputDBName = Constants.inputDBName
  private val outputDBName = Constants.outputDBName
  private val outputTableName = "edr_top_subs_table"
  private val outputFormat = Constants.outputFormat
  private val subscriberColName = Constants.subscriberColName
  private val selectCols = Seq(subscriberColName, downLoadColName, upLoadColName, "hour")
  private val groupByCols = Seq("hour", subscriberColName)
  private val aggCols = Seq(upLoadColName, downLoadColName, totalBytesColName)
  private val outputPartitionCols = Seq("hour")

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {
      spark = LteUtils.getSparkSession()
      val inputDF = LteUtils.readHiveTable(spark, inputDBName, inputTableName, selectCols)
      logger.info("Schema of inputDF" + inputDF.schema)

      val dfWithTotalBytesColumn = LteUtils.sumColumns(inputDF, downLoadColName, upLoadColName, totalBytesColName)

      val aggregatedDF = LteUtils.getAggregatedDF(dfWithTotalBytesColumn, groupByCols, aggCols)
      logger.info("Schema of aggregatedDF" + aggregatedDF.schema)

      aggregatedDF.persist(StorageLevel.MEMORY_AND_DISK)

      val downLoadRankedDF = getRankedDF(aggregatedDF, "sum_"+downLoadColName)
      val upLoadRankedDF = getRankedDF(aggregatedDF, "sum_"+upLoadColName)
      val totalBytesRankedDF = getRankedDF(aggregatedDF, "sum_"+totalBytesColName)

      val outputDF = downLoadRankedDF.join(upLoadRankedDF, "hour").join(totalBytesRankedDF, "hour")

      logger.info("Schema of outputDF" + outputDF.schema)

      LteUtils.writeOutputDF(outputDF, outputPartitionCols, outputFormat, outputDBName, outputTableName)
    }
    catch {
      case ex: Exception =>
        logger.error("Error occurred while executing main method in TopSubscribersJob")
        logger.error("StackTrace -> {}", ExceptionUtils.getRootCauseStackTrace(ex).mkString("\n"))
        throw ex
    } finally {
      LteUtils.closeSparkSession(spark)
    }
  }

  def getWindowSpec(partitionCol: String, orderByCol: String): WindowSpec =
  {
    val windowSpec = Window.partitionBy(partitionCol).orderBy(col(orderByCol).desc)
    windowSpec
  }

  def getDenseRank(windowSpec: WindowSpec): Column =
  {
    val denseRank = dense_rank().over(windowSpec)
    denseRank
  }

  def getRankedDF(aggregatedDF: DataFrame,colName: String): DataFrame =
  {
    val windowSpec = getWindowSpec("hour", colName)
    val dense_rank = getDenseRank(windowSpec)
    val dfWithDenseRank = aggregatedDF.select(col("hour"), col(subscriberColName), col(colName), dense_rank.as("rank")).filter(col("rank") <= 10)
    val dfWithTopSubsList = dfWithDenseRank.select(col("hour"), col(subscriberColName)).groupBy(col("hour")).agg(collect_set(col(subscriberColName)).as("max_"+colName))
    dfWithTopSubsList
  }

}

