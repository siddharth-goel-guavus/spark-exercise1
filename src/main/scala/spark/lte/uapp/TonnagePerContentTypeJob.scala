//This Job computes the tonnage per website domain url, per content_type, % contribution of content_type in overall tonnage, per minute.

package spark.lte.uapp

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, sum}
import org.slf4j.LoggerFactory

object TonnagePerContentTypeJob {
  private val logger = LoggerFactory.getLogger(getClass)
  private val downLoadColName = Constants.downLoadColName
  private val upLoadColName = Constants.upLoadColName
  private val totalBytesColName = Constants.totalBytesColName
  private val tableName = Constants.tableName
  private val dbName = Constants.dbName
  private val httpUrlColName = Constants.httpUrlColName
  private val domainNameColName = Constants.domainNameColName
  private val httpContentTypeColName = Constants.httpContentTypeColName
  private val httpContentTypeDataPath = "/data/siddharth/http_content_type_data.csv"
  private val selectCols = Seq("hour", "minute", upLoadColName, downLoadColName, httpUrlColName, httpContentTypeColName)

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {
      spark = LteUtils.getSparkSession()
      val inputDF = LteUtils.readORCData(spark, dbName, tableName, selectCols)

      logger.info("Schema of inputDF" + inputDF.schema)

      val dfWithDomainName = inputDF.
        filter(col(httpUrlColName).isNotNull).
        withColumn(domainNameColName, LteUtils.getDomainUdf(inputDF.col(httpUrlColName))).
        drop(col(httpUrlColName))

      val dfWithContentTypeCol = dfWithDomainName.
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
        groupBy(col("hour"), col("minute"), col(domainNameColName), col(httpContentTypeColName)).
        agg(sum(col(totalBytesColName)).as("tonnage"))

      val aggregatedDfWithPercentage = aggregatedDF.withColumn("percent_tonnage", col("tonnage") / sum("tonnage").
        over(Window.partitionBy(col("hour"), col("minute"), aggregatedDF.col(domainNameColName))) * 100)

      aggregatedDfWithPercentage.write.
        partitionBy("hour", "minute").
        format("orc").
        saveAsTable("sid_output_db.edr_tonnage_per_content_table")
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


}

