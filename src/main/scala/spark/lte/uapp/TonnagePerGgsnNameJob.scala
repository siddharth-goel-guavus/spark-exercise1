//This job computes tonnage per ggsn-name using ggsn.xml dataset per minute

package spark.lte.uapp

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, sum, explode, broadcast}
import org.slf4j.LoggerFactory
import com.databricks.spark.xml._

object TonnagePerGgsnNameJob {
  private val logger = LoggerFactory.getLogger(getClass)
  private val ggsnXMLPath = Constants.ggsnXMLPath
  private val downLoadColName = Constants.downLoadColName
  private val upLoadColName = Constants.upLoadColName
  private val totalBytesColName = Constants.totalBytesColName
  private val ggsnIPColName = Constants.ggsnIPColName
  private val ggsnNameColName = Constants.ggsnNameColName
  private val inputTableName = Constants.inputTableName
  private val inputDBName = Constants.inputDBName
  private val selectCols = Seq("hour", "minute", upLoadColName, downLoadColName, ggsnIPColName)
  private val outputDBName = Constants.outputDBName
  private val outputFormat = Constants.outputFormat
  private val groupByCols = Seq("hour", "minute", ggsnNameColName)
  private val outputTableName = "edr_tonnage_per_ggsn_table"
  private val outputPartitionCols = Seq("hour", "minute")

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {
      spark = LteUtils.getSparkSession()
      val inputDF = LteUtils.readHiveTable(spark, inputDBName, inputTableName, selectCols)
      logger.info("Schema of inputDF" + inputDF.schema)

      val ggsnDF = readGgsnXml(spark, ggsnXMLPath, ggsnNameColName, ggsnIPColName)
      logger.info("Schema of ggsnDF" + ggsnDF.schema)

      val joinedDF = inputDF.join(broadcast(ggsnDF), ggsnIPColName).drop(col(ggsnIPColName))

      val dfWithTotalBytesColumn = LteUtils.sumColumns(joinedDF, downLoadColName, upLoadColName, totalBytesColName)

      val aggregatedDF = dfWithTotalBytesColumn.
        groupBy(groupByCols.map(c => col(c)): _*).
        agg(sum(col(totalBytesColName)).as("tonnage"))

      LteUtils.writeOutputDF(aggregatedDF, outputPartitionCols, outputFormat, outputDBName, outputTableName)
    }
    catch {
      case ex: Exception =>
        logger.error("Error occurred while executing main method in TonnagePerGgsnNameJob")
        logger.error("StackTrace -> {}", ExceptionUtils.getRootCauseStackTrace(ex).mkString("\n"))
        throw ex
    } finally {
      LteUtils.closeSparkSession(spark)
    }
  }

  def readGgsnXml(sparkSession: SparkSession, XMLpath: String, ggsnNameColName: String, ggsnIPColName: String): DataFrame = {
    val df = sparkSession.read.option("rowTag", "ggsn").option("attributePrefix", "prefix_").option("valueTag", "val").xml(XMLpath)
    val explodedf = df.select(col("prefix_name").as(ggsnNameColName), explode(col("rule")).as("rule_col"))
    val ggsnDF = explodedf.select(col(ggsnNameColName), col("rule_col.condition.prefix_value").as(ggsnIPColName))

    ggsnDF
  }

}

