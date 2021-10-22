// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Overview
// MAGIC 
// MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
// MAGIC 
// MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

// COMMAND ----------

// MAGIC %scala
// MAGIC import java.text.SimpleDateFormat
// MAGIC import java.util.Date
// MAGIC 
// MAGIC val file_location = "/FileStore/tables/2015_07_22_mktplace_shop_web_log_sample-1.log"
// MAGIC val file_type = "log"
// MAGIC case class Log(date_time: String, elb: String, client_ip: String, backend_ip:String ,request_processing_time: String, backend_processing_time: String
// MAGIC                        , response_processing_time:String, elb_status_code:String, backend_status_code:String, received_bytes:String
// MAGIC                        , sent_bytes:String, request:String, user_agent:String, ssl_cipher:String
// MAGIC                        , ssl_protocol:String)
// MAGIC 
// MAGIC val PATTERN = """([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+ \S+ \S+)" "([^"]*)" (\S+) (\S+)""".r
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC def parseLogLine(log: String):Log = {
// MAGIC     
// MAGIC   val PATTERN(date_time, elb, client_ip, backend_ip,request_processing_time, backend_processing_time
// MAGIC                        , response_processing_time, elb_status_code, backend_status_code, received_bytes
// MAGIC                        , sent_bytes, request, user_agent, ssl_cipher, ssl_protocol) = log
// MAGIC   Log(date_time, elb, client_ip, backend_ip,request_processing_time, backend_processing_time
// MAGIC                        , response_processing_time, elb_status_code, backend_status_code, received_bytes
// MAGIC                        , sent_bytes, request, user_agent, ssl_cipher, ssl_protocol)
// MAGIC }
// MAGIC 
// MAGIC val rdd: RDD[Log] = sc
// MAGIC               .textFile(file_location)
// MAGIC               .map{parseLogLine}
// MAGIC 
// MAGIC import org.apache.spark.sql.expressions.Window
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import spark.implicits._
// MAGIC val dateUdf = udf( (date: String) => {
// MAGIC      val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
// MAGIC      val dt = df.parse(date);
// MAGIC      val epoch = dt.getTime();
// MAGIC      (epoch)
// MAGIC  })
// MAGIC 
// MAGIC 
// MAGIC val df = rdd.toDF().withColumn("timestamp", dateUdf($"date_time"))
// MAGIC val windowSpec = Window.partitionBy("client_ip").orderBy("timestamp")
// MAGIC val isNewSession = udf((duration: Long) => {
// MAGIC       if (duration > 900000) 1
// MAGIC       else 0
// MAGIC  })
// MAGIC    
// MAGIC val getConcatenated = udf( (first: String, second: String) => { first + "_" + second } )
// MAGIC    
// MAGIC val dfWithTimestamp= df.withColumn("prev_timestamp",lag(df("timestamp"), 1).over(windowSpec))
// MAGIC     
// MAGIC val dfWithTimestampCleaned= dfWithTimestamp.withColumn("prev_timestamp_cleaned", coalesce('prev_timestamp, 'timestamp))
// MAGIC     
// MAGIC val dfWithDuration= dfWithTimestampCleaned.withColumn("duration_miliseconds",dfWithTimestampCleaned("timestamp")-dfWithTimestampCleaned("prev_timestamp_cleaned"))
// MAGIC     
// MAGIC val dfWithNewSessionFlag= dfWithDuration.withColumn("is_new_session",isNewSession($"duration_miliseconds"))
// MAGIC     
// MAGIC val dfWithWindowIdx=dfWithNewSessionFlag.withColumn("windowIdx",sum("is_new_session").over(windowSpec).cast("string"))
// MAGIC     
// MAGIC val dfWithSessionId=dfWithWindowIdx.withColumn("session_id",getConcatenated($"client_ip",$"windowIdx")).select($"client_ip",$"session_id",$"duration_miliseconds",$"request",$"user_agent").cache()
// MAGIC 
// MAGIC //Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
// MAGIC dfWithSessionId.show()
// MAGIC     
// MAGIC //Determine the average session time
// MAGIC dfWithSessionId.select(mean("duration_miliseconds")).show()
// MAGIC     
// MAGIC //Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
// MAGIC val urlVisitsPerSessionBase=dfWithSessionId.select("session_id","request").groupBy('session_id).agg((collect_set("request")))
// MAGIC val urlVisitsPerSession= urlVisitsPerSessionBase.select($"session_id",size($"collect_set(request)"))
// MAGIC urlVisitsPerSession.show()
// MAGIC     
// MAGIC //Find the most engaged users, ie the IPs with the longest session times
// MAGIC dfWithSessionId.groupBy("client_ip").sum("duration_miliseconds").sort($"sum(duration_miliseconds)".desc).show()
// MAGIC     
