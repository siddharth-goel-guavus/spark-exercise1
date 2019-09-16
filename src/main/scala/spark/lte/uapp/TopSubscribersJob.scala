// This job computes the top 10 subscribers by download bytes, upload bytes and sum(down + up) per hour.

package spark.lte.uapp

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, dense_rank, lit, sum}
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.storage.StorageLevel

object TopSubscribersJob {
  private val logger = LoggerFactory.getLogger(getClass)
  private val downLoadColName = Constants.downLoadColName
  private val upLoadColName = Constants.upLoadColName
  private val totalBytesColName = Constants.totalBytesColName
  private val tableName = Constants.tableName
  private val dbName = Constants.dbName
  private val subscriberColName = Constants.subscriberColName
  private val selectCols = Seq(subscriberColName, downLoadColName, upLoadColName, "hour")

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {
      spark = LteUtils.getSparkSession()
      val inputDF = LteUtils.readORCData(spark, dbName, tableName, selectCols)

      logger.info("Schema of inputDF" + inputDF.schema)

      val dfWithTotalBytesColumn = LteUtils.sumColumns(inputDF, downLoadColName, upLoadColName, totalBytesColName)

      val aggregatedDF = dfWithTotalBytesColumn.
        groupBy(col("hour"), col(subscriberColName)).
        agg(sum(col(upLoadColName)).as("sum_" + upLoadColName),
          sum(col(downLoadColName)).as("sum_" + downLoadColName),
          sum(col(totalBytesColName)).as("sum_" + totalBytesColName))

      logger.info("Schema of aggregatedDF" + aggregatedDF.schema)

      aggregatedDF.persist(StorageLevel.MEMORY_AND_DISK)

      val downLoadRankedDF = getRankedDF(aggregatedDF, "sum_"+downLoadColName)
      val upLoadRankedDF = getRankedDF(aggregatedDF, "sum_"+upLoadColName)
      val totalBytesRankedDF = getRankedDF(aggregatedDF, "sum_"+totalBytesColName)
      //df.write.in.hive

      val outputDF : DataFrame = downLoadRankedDF.union(upLoadRankedDF).union(totalBytesRankedDF)

      logger.info("Schema of outputDF" + outputDF.schema)

      outputDF.write.
        partitionBy("hour").
        format("orc").
        saveAsTable("sid_output_db.edr_top_subs_table")
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
    val dfWithRankTypeCol = dfWithDenseRank.withColumn("rank_type", lit(colName))
    dfWithRankTypeCol
  }

}

