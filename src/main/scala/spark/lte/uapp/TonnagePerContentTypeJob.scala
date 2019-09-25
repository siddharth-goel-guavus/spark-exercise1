//This Job computes the tonnage per website domain url, per content_type, % contribution of content_type in overall tonnage, per minute.

package spark.lte.uapp

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{broadcast, col, sum}
import org.slf4j.LoggerFactory

object TonnagePerContentTypeJob {
  private val logger = LoggerFactory.getLogger(getClass)
  private val downLoadColName = Constants.downLoadColName
  private val upLoadColName = Constants.upLoadColName
  private val totalBytesColName = Constants.totalBytesColName
  private val inputTableName = Constants.inputTableName
  private val inputDBName = Constants.inputDBName
  private val outputDBName = Constants.outputDBName
  private val outputTableName = "edr_tonnage_per_content_table"
  private val httpUrlColName = Constants.httpUrlColName
  private val domainNameColName = Constants.domainNameColName
  private val httpContentTypeColName = Constants.httpContentTypeColName
  private val httpContentTypeDataPath = "/data/siddharth/http_content_type_data.csv"
  private val selectCols = Seq("hour", "minute", upLoadColName, downLoadColName, httpUrlColName, httpContentTypeColName)
  private val outputPartitionCols = Seq("hour", "minute")
  private val outputFormat = Constants.outputFormat
  private val groupByCols = Seq("hour", "minute", domainNameColName, httpContentTypeColName)
  private val windowPartitionCols = Seq("hour", "minute", domainNameColName)

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {
      spark = LteUtils.getSparkSession()
      val inputDF = LteUtils.readHiveTable(spark, inputDBName, inputTableName, selectCols)

      logger.info("Schema of inputDF" + inputDF.schema)

      val dfWithDomainName = inputDF.
        filter(col(httpUrlColName).isNotNull).
        withColumn(domainNameColName, LteUtils.getDomainUdf(inputDF.col(httpUrlColName))).
        drop(col(httpUrlColName))

      val dfWithContentTypeCol = dfWithDomainName.
        filter(col(httpContentTypeColName).isNotNull).
        withColumn(httpContentTypeColName, LteUtils.getContentTypeUdf(col(httpContentTypeColName))).
        withColumn(httpContentTypeColName, LteUtils.removeQuotesUdf(col(httpContentTypeColName)))

      val contentTypeTable = spark.read.csv(httpContentTypeDataPath)

      val joinedDf = dfWithContentTypeCol.join(broadcast(contentTypeTable), col(httpContentTypeColName) === col("_c0"))

      val dfWithContentValues = joinedDf.dropDuplicates.drop("_c0", httpContentTypeColName).withColumnRenamed("_c1", httpContentTypeColName)

      val dfWithTotalBytesColumn = LteUtils.sumColumns(dfWithContentValues, downLoadColName, upLoadColName, totalBytesColName)

      val aggregatedDF = dfWithTotalBytesColumn.
        groupBy(groupByCols.map(c => col(c)): _*).
        agg(sum(col(totalBytesColName)).as("tonnage"))

      val aggregatedDfWithPercentage = aggregatedDF.withColumn("percent_tonnage", col("tonnage") / sum("tonnage").
        over(getWindowSpec(windowPartitionCols)) * 100)

      LteUtils.writeOutputDF(aggregatedDfWithPercentage, outputPartitionCols, outputFormat, outputDBName, outputTableName)
    }
    catch {
      case ex: Exception =>
        logger.error("Error occurred while executing main method in TonnagePerContentTypeJob")
        logger.error("StackTrace -> {}", ExceptionUtils.getRootCauseStackTrace(ex).mkString("\n"))
        throw ex
    } finally {
      LteUtils.closeSparkSession(spark)
    }
  }

  def getWindowSpec(partitionCols: Seq[String]): WindowSpec =
  {
    Window.partitionBy(partitionCols.map(c => col(c)): _*)
  }

}

