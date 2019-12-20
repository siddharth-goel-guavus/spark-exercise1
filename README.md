# spark-exercise1

Properties File Location: src/main/resources/sparkJobs.properties

## Spark Jobs Classes: 

<li> src/main/scala/spark/lte/uapp/TopSubscribersJob.scala </li>
<li> src/main/scala/spark/lte/uapp/TopContentTypesJob.scala </li>
<li> src/main/scala/spark/lte/uapp/TonnagePerDomainJob.scala </li>
<li> src/main/scala/spark/lte/uapp/TonnagePerContentTypeJob.scala </li>
<li> src/main/scala/spark/lte/uapp/TonnagePerGgsnNameJob.scala </li>

Constants File: src/main/scala/spark/lte/uapp/Constants.scala

Utils Class: src/main/scala/spark/lte/uapp/LteUtils.scala

Content Type Dictionary: http_content_type_data.csv

Run Command: 
```/usr/hdp/2.6.5.0-292/spark2/bin/spark-submit --class spark.lte.uapp.ClassName --properties-file propertiesFilePath.properties JarPath.jar```

Input DB Name: sid_db
Input Table Name: edr_tab_main
Output DB Name: sid_output_db
