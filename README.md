# spark-exercise1

Properties File Location: src/main/resources/sparkJobs.properties

Spark Jobs Classes: 
1 - src/main/scala/spark/lte/uapp/TopSubscribersJob.scala
2 - src/main/scala/spark/lte/uapp/TopContentTypesJob.scala
3 - src/main/scala/spark/lte/uapp/TonnagePerDomainJob.scala
4 - src/main/scala/spark/lte/uapp/TonnagePerContentTypeJob.scala
5 - src/main/scala/spark/lte/uapp/TonnagePerGgsnNameJob.scala

Constants File: src/main/scala/spark/lte/uapp/Constants.scala

Utils Class: src/main/scala/spark/lte/uapp/LteUtils.scala

Run Command: 
/usr/hdp/2.6.5.0-292/spark2/bin/spark-submit --class spark.lte.uapp.ClassName --properties-file propertiesFilePath.properties JarPath.jar

Input DB Name: sid_db
Input Table Name: edr_tab_main
Output DB Name: sid_output_db
