//This job computes tonnage (amount of data used(down + up)), count of hits per domain url(UDF to compute) per minute.

package spark.lte.uapp

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, sum}
import org.slf4j.LoggerFactory

object TonnagePerDomainJob {
  private val logger = LoggerFactory.getLogger(getClass)
  private val downLoadColName = Constants.downLoadColName
  private val upLoadColName = Constants.upLoadColName
  private val totalBytesColName = Constants.totalBytesColName
  private val inputTableName = Constants.inputTableName
  private val inputDBName = Constants.inputDBName
  private val replyCodeColName = Constants.replyCodeColName
  private val httpUrlColName = Constants.httpUrlColName
  private val domainNameColName = Constants.domainNameColName
  private val urlHitsColName = Constants.urlHitsColName
  private val selectCols = Seq("hour", "minute", upLoadColName, downLoadColName, replyCodeColName, httpUrlColName)
  private val outputDBName = Constants.outputDBName
  private val outputTableName = "edr_tonnage_per_domain_table"
  private val outputFormat = Constants.outputFormat
  private val outputPartitionCols = Seq("hour", "minute")
  private val groupByCols = Seq("hour", "minute", domainNameColName)
  private val regexString = "^2[0-9][0-9]$"

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {
      spark = LteUtils.getSparkSession()
      val inputDF = LteUtils.readHiveTable(spark, inputDBName, inputTableName, selectCols)

      logger.info("Schema of inputDF" + inputDF.schema)

      val dfWithSuccessfulHits = getDFWithSuccessfulHits(inputDF, regexString, replyCodeColName)

      val dfWithDomainName = dfWithSuccessfulHits.
        filter(col(httpUrlColName).isNotNull).
        withColumn(domainNameColName, LteUtils.getDomainUdf(dfWithSuccessfulHits.col(httpUrlColName))).
        drop(col(httpUrlColName))

      val dfWithTotalBytesColumn = LteUtils.sumColumns(dfWithDomainName, downLoadColName, upLoadColName, totalBytesColName)

      val aggregatedDF = dfWithTotalBytesColumn.
        groupBy(groupByCols.map(c => col(c)): _*).
        agg(sum(col(totalBytesColName)).as("tonnage"), count(col(replyCodeColName)).as(urlHitsColName))

      LteUtils.writeOutputDF(aggregatedDF, outputPartitionCols, outputFormat, outputDBName, outputTableName)
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

  def getDFWithSuccessfulHits(inputDF: DataFrame, regexString: String, filterCol: String): DataFrame =
  {
    inputDF.filter(col(filterCol).rlike(regexString))
  }

}

