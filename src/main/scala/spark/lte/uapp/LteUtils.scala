package spark.lte.uapp

import java.util.Objects

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, to_timestamp, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Success, Try}

object LteUtils {

  private val logger = LoggerFactory.getLogger(getClass)

  def getSparkSession(): SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()

  def readORCData(sparkSession: SparkSession, dbName: String, tableName: String, selectCols: Seq[String]): DataFrame =
  {
    val df = sparkSession.sql("select * from " + dbName + "." + tableName)
    val dfWithSelectCols = df.select(selectCols.map(c => col(c)): _*)
    dfWithSelectCols
  }

  def convertStringColsToTimestamp(sparkSession: SparkSession, inputDF: DataFrame, timeStampCols: List[String]): DataFrame =
  {
    var outputDF = inputDF
    timeStampCols.foreach(item =>
      {
        val timeStampTransform = to_timestamp(col(item), "MM/dd/yyyy HH:mm:ss")
        outputDF = outputDF.withColumn("new_" + item, timeStampTransform).drop(col(item)).withColumnRenamed("new_" + item, item)
      }
    )
    outputDF
  }

  def storeInHive(sparkSession: SparkSession, inputDf: DataFrame, dbName :String, tableName: String): Unit =
  {
    sparkSession.sql("INSERT INTO TABLE " + tableName + " PARTITION (part_col_name) SELECT *, year(to_date(my_timestamp_column)) FROM my_not_partitioned_table");
  }

  def closeSparkSession(sparkSession: SparkSession): Unit = {
    if (Objects.nonNull(sparkSession)) {
      logger.info("Attempting to close SparkSession resource handle")
      sparkSession.close()
    } else {
      logger.info("SparkSession is null")
    }
  }

  val addNums : (Int, Int) => Int = (num1 : Int, num2 : Int) => {num1 + num2}

  val addColumnUDF = udf(addNums)

  def sumColumns(inputDF: DataFrame, col1: String, col2: String, totalBytesColName: String): DataFrame =
  {
    val outputDF = inputDF.withColumn(totalBytesColName, addColumnUDF(inputDF.col(col1), inputDF.col(col2)))
    outputDF
  }

  //Tonnage Jobs Utils

  val checkInt: (String) => Boolean = (x: String) => { val y = Try(x.toInt); y match { case Success(x) => true; case _ => false; }}

  val checkIntUdf: UserDefinedFunction= udf(checkInt)

  val getDomainUdf: UserDefinedFunction = udf(getDomain _)

  val getSuccessfulResponseCodeUdf: UserDefinedFunction = udf(getSuccessfulResponseCode _)

  val removeQuotes : (String) => String = (str: String) => str.replaceAll("^\"|\"$", "")

  val removeQuotesUdf: UserDefinedFunction = udf(removeQuotes)

  val getContentTypeUdf: UserDefinedFunction = udf(getContentType _)

  def getDomain(url: String): String = {
    val splitArray = url.split("/")
    if(splitArray.asInstanceOf[Array[String]].size <=2)
    {
      return ""
    }
    splitArray(2)
  }

  def getSuccessfulResponseCode(x: String): Boolean = {
    val y = x.toInt
    if(y >= 200 && y < 300)
    {return true}
    false
  }

  def getContentType(content_type: String): String = {
    val splitArray = content_type.toLowerCase.split(";")
    splitArray(0)
  }

}
