/* This job gives a dataset in hive with top 5 content types(for e.g. audio, text, pdf , json, video, image, byte Stream etc.)
 per subscriber (radius_user_name) in terms of hits, bytes downloaded and bytes uploaded. */

package spark.lte.uapp

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, dense_rank, collect_set}
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.storage.StorageLevel

object TopContentTypesJob {

  private val downLoadColName = Constants.downLoadColName
  private val upLoadColName = Constants.upLoadColName
  private val totalBytesColName = Constants.totalBytesColName
  private val inputTableName = Constants.inputTableName
  private val inputDBName = Constants.inputDBName
  private val httpContentTypeColName = Constants.httpContentTypeColName
  private val subscriberColName = Constants.subscriberColName
  private val logger = LoggerFactory.getLogger(getClass)
  private val httpContentTypeDataPath = Constants.httpContentTypeDataPath
  private val selectCols = Seq("hour", "minute", subscriberColName, httpContentTypeColName, downLoadColName, upLoadColName)
  private val groupByCols = Seq("hour", "minute", subscriberColName, httpContentTypeColName)
  private val aggCols = Seq(downLoadColName, upLoadColName, totalBytesColName)
  private val outputDBName = Constants.outputDBName
  private val outputFormat = Constants.outputFormat
  private val outputPartitionCols = Seq("hour", "minute")
  private val outputTableName = "edr_top_content_type_table"

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {
      spark = LteUtils.getSparkSession()
      val inputDF = LteUtils.readHiveTable(spark, inputDBName, inputTableName, selectCols)

      logger.info("Schema of inputDF" + inputDF.schema)

      val dfWithContentTypeCol = inputDF.
        filter(col(httpContentTypeColName).isNotNull).
        withColumn(httpContentTypeColName, LteUtils.getContentTypeUdf(col(httpContentTypeColName))).
        withColumn(httpContentTypeColName, LteUtils.removeQuotesUdf(col(httpContentTypeColName)))

      val contentTypeTable = spark.read.csv(httpContentTypeDataPath)

      val joinedDf = dfWithContentTypeCol.join(broadcast(contentTypeTable), col(httpContentTypeColName) === col("_c0"))

      val dfWithContentValues = joinedDf.dropDuplicates.drop("_c0", httpContentTypeColName).withColumnRenamed("_c1", httpContentTypeColName)

      val dfWithTotalBytesColumn = LteUtils.sumColumns(dfWithContentValues, downLoadColName, upLoadColName, totalBytesColName)

      val aggregatedDF = LteUtils.getAggregatedDF(dfWithTotalBytesColumn, groupByCols, aggCols)

      logger.info("Schema of aggregatedDF" + aggregatedDF.schema)

      aggregatedDF.persist(StorageLevel.MEMORY_AND_DISK)

      val downLoadRankedDF = getRankedDF(aggregatedDF, "sum_"+downLoadColName)
      val upLoadRankedDF = getRankedDF(aggregatedDF, "sum_"+upLoadColName)
      val totalBytesRankedDF = getRankedDF(aggregatedDF, "sum_"+totalBytesColName)

      val outputDF = downLoadRankedDF.join(upLoadRankedDF, Seq("hour", "minute", subscriberColName)).join(totalBytesRankedDF, Seq("hour", "minute", subscriberColName))

      logger.info("Schema of outputDF" + outputDF.schema)

      LteUtils.writeOutputDF(outputDF, outputPartitionCols, outputFormat, outputDBName, outputTableName)
    }
    catch {
      case ex: Exception =>
        logger.error("Error occurred while executing main method in TopContentTypesJob")
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
    val dfWithTopSubsList = dfWithDenseRank.select(col("hour"), col("minute"),col(subscriberColName), col(httpContentTypeColName)).groupBy(col("hour"), col("minute"), col(subscriberColName)).agg(collect_set(col(httpContentTypeColName)).as("max_"+colName))
    dfWithTopSubsList
  }

}

