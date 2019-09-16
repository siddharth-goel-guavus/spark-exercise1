// This job gives a dataset in hive with top 5 content types(for e.g. audio, text, pdf , json, video, image, byte Stream etc.)
// per subscriber (radius_user_name) in terms of hits, bytes downloaded and bytes uploaded.

package spark.lte.uapp

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, dense_rank, lit, sum}
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.storage.StorageLevel

object TopContentTypesJob {

  private val downLoadColName = Constants.downLoadColName
  private val upLoadColName = Constants.upLoadColName
  private val totalBytesColName = Constants.totalBytesColName
  private val tableName = Constants.tableName
  private val dbName = Constants.dbName
  private val httpContentTypeColName = Constants.httpContentTypeColName
  private val subscriberColName = Constants.subscriberColName
  private val logger = LoggerFactory.getLogger(getClass)
  private val httpContentTypeDataPath = Constants.httpContentTypeDataPath
  private val selectCols = Seq("hour", "minute", subscriberColName, httpContentTypeColName, downLoadColName, upLoadColName)

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {
      spark = LteUtils.getSparkSession()
      val inputDF = LteUtils.readORCData(spark, dbName, tableName, selectCols)

      logger.info("Schema of inputDF" + inputDF.schema)

      val dfWithContentTypeCol = inputDF.
        filter(col(httpContentTypeColName).isNotNull).
        withColumn(httpContentTypeColName + "_new", LteUtils.getContentTypeUdf(col(httpContentTypeColName))).
        drop(httpContentTypeColName).
        withColumnRenamed(httpContentTypeColName + "_new", httpContentTypeColName).
        withColumn(httpContentTypeColName + "_new", LteUtils.removeQuotesUdf(col(httpContentTypeColName))).
        drop(httpContentTypeColName).
        withColumnRenamed(httpContentTypeColName + "_new", httpContentTypeColName)

      val contentTypeTable = spark.read.csv(httpContentTypeDataPath)

      val joinedDf = dfWithContentTypeCol.join(broadcast(contentTypeTable), col(httpContentTypeColName) === col("_c0"))

      val dfWithContentValues = joinedDf.dropDuplicates.drop("_c0", httpContentTypeColName).withColumnRenamed("_c1", httpContentTypeColName)

      val dfWithTotalBytesColumn = LteUtils.sumColumns(dfWithContentValues, downLoadColName, upLoadColName, totalBytesColName)

      val aggregatedDF = dfWithTotalBytesColumn.
        groupBy(col("hour"), col("minute"), col(subscriberColName), col(httpContentTypeColName)).
        agg(sum(col(upLoadColName)).as("sum_" + upLoadColName),
          sum(col(downLoadColName)).as("sum_" + downLoadColName),
          sum(col(totalBytesColName)).as("sum_" + totalBytesColName))

      logger.info("Schema of aggregatedDF" + aggregatedDF.schema)

      aggregatedDF.persist(StorageLevel.MEMORY_AND_DISK)

      val downLoadRankedDF = getRankedDF(aggregatedDF, "sum_"+downLoadColName)
      val upLoadRankedDF = getRankedDF(aggregatedDF, "sum_"+upLoadColName)
      val totalBytesRankedDF = getRankedDF(aggregatedDF, "sum_"+totalBytesColName)

      val outputDF : DataFrame = downLoadRankedDF.union(upLoadRankedDF).union(totalBytesRankedDF)

      logger.info("Schema of outputDF" + outputDF.schema)

      outputDF.write.
        partitionBy("hour", "minute").
        format("orc").
        saveAsTable("sid_output_db.edr_top_content_type_table")
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

  def getWindowSpec(orderByCol: String): WindowSpec =
  {
    val windowSpec = Window.partitionBy("hour", "minute", subscriberColName).orderBy(col(orderByCol).desc)
    windowSpec
  }

  def getDenseRank(windowSpec: WindowSpec): Column =
  {
    val denseRank = dense_rank().over(windowSpec)
    denseRank
  }

  def getRankedDF(aggregatedDF: DataFrame,colName: String): DataFrame =
  {
    val windowSpec = getWindowSpec(colName)
    val dense_rank = getDenseRank(windowSpec)
    val dfWithDenseRank = aggregatedDF.select(col("hour"), col("minute"), col(subscriberColName), col(httpContentTypeColName), col(colName), dense_rank.as("rank")).filter(col("rank") <= 5)
    val dfWithRankTypeCol = dfWithDenseRank.withColumn("rank_type", lit(colName))
    dfWithRankTypeCol
  }

}

