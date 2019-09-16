//This job computes tonnage (amount of data used(down + up)), count of hits per domain url(UDF to compute) per minute.

package spark.lte.uapp

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, count}
import org.slf4j.LoggerFactory

object TonnagePerDomainJob {
  private val logger = LoggerFactory.getLogger(getClass)
  private val downLoadColName = Constants.downLoadColName
  private val upLoadColName = Constants.upLoadColName
  private val totalBytesColName = Constants.totalBytesColName
  private val tableName = Constants.tableName
  private val dbName = Constants.dbName
  private val replyCodeColName = Constants.replyCodeColName
  private val httpUrlColName = Constants.httpUrlColName
  private val domainNameColName = Constants.domainNameColName
  private val urlHitsColName = Constants.urlHitsColName
  private val selectCols = Seq("hour", "minute", upLoadColName, downLoadColName, replyCodeColName, httpUrlColName)

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {
      spark = LteUtils.getSparkSession()
      val inputDF = LteUtils.readORCData(spark, dbName, tableName, selectCols)

      logger.info("Schema of inputDF" + inputDF.schema)

      val dfWithSuccessfulHits = inputDF.filter(col(replyCodeColName).rlike("^2[0-9][0-9]$"))

      val dfWithDomainName = dfWithSuccessfulHits.
        filter(col(httpUrlColName).isNotNull).
        withColumn(domainNameColName, LteUtils.getDomainUdf(dfWithSuccessfulHits.col(httpUrlColName))).
        drop(col(httpUrlColName))

      val dfWithTotalBytesColumn = LteUtils.sumColumns(dfWithDomainName, downLoadColName, upLoadColName, totalBytesColName)

      val aggregatedDF = dfWithTotalBytesColumn.
        groupBy(col("hour"), col("minute"),col(domainNameColName)).
        agg(sum(col(totalBytesColName)).as("tonnage"), count(col(replyCodeColName)).as(urlHitsColName))

      aggregatedDF.write.
        partitionBy("hour","minute").
        format("orc").
        saveAsTable("sid_output_db.edr_tonnage_per_domain_table")
    }
    catch {
      case ex: Exception =>
        logger.error("Error occurred while executing main method in TonnagePerDomainJob")
        logger.error("StackTrace -> {}", ExceptionUtils.getRootCauseStackTrace(ex).mkString("\n"))
        throw ex
    } finally {
      LteUtils.closeSparkSession(spark)
    }
  }


}

