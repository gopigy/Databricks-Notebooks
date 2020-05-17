// Databricks notebook source
// MAGIC %scala
// MAGIC 
// MAGIC import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
// MAGIC import java.util.Calendar
// MAGIC import java.util.TimeZone
// MAGIC import java.text.SimpleDateFormat
// MAGIC import java.time.ZonedDateTime
// MAGIC import java.time.Instant
// MAGIC import java.nio.file.Paths
// MAGIC 
// MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
// MAGIC 
// MAGIC val metaUrl = dbutils.widgets.get("metaurl")
// MAGIC val metaUser = dbutils.widgets.get("metauser")
// MAGIC //dev-secrets,sqldb-password
// MAGIC val metaPass = dbutils.secrets.get(dbutils.widgets.get("secretvault"),dbutils.widgets.get("metapass"))
// MAGIC val verificationType = dbutils.widgets.get("verificationtype")
// MAGIC val loadType = dbutils.widgets.get("loadtype")
// MAGIC val logPath = dbutils.widgets.get("logpath")
// MAGIC val prevActivityStatus = dbutils.widgets.get("activitystatus")
// MAGIC val partitionDt = dbutils.widgets.get("partitiondt")
// MAGIC 
// MAGIC var inputCount = 0L
// MAGIC var outputCount = 0L
// MAGIC var viewMaxPartitionDt = ""
// MAGIC var prevCount = 0L
// MAGIC 
// MAGIC def GetLogPath(verify: String, path: String, yearmonth: String, day: String): String = {
// MAGIC    val count = 0
// MAGIC    val out = Paths.get(path, yearmonth, day, verify + "_count_" + count + ".log").toString().replaceAll("^wasb:/", "wasb://")
// MAGIC    println("Log file path:" + out)
// MAGIC    out
// MAGIC  }
// MAGIC 
// MAGIC def TimeStampNow(): Timestamp = {
// MAGIC   val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
// MAGIC   val today = Calendar.getInstance().getTime
// MAGIC   val now = timeFormat.format(today)
// MAGIC   val ts = java.sql.Timestamp.valueOf(now)
// MAGIC   ts
// MAGIC }
// MAGIC 
// MAGIC def StringToTimeStamp(str: String): Timestamp = {
// MAGIC   val zdt = Instant.parse(str)
// MAGIC   val ts = java.sql.Timestamp.from(zdt)
// MAGIC   ts
// MAGIC }
// MAGIC 
// MAGIC def ReadCounts(log: String) {
// MAGIC   val contents = dbutils.fs.head(log)
// MAGIC   val fileSplit = contents.split("\n")
// MAGIC   fileSplit.foreach(line => {
// MAGIC     if(line.contains("input_count=")){
// MAGIC       var l = line.replace("input_count=", "")
// MAGIC       l = l.trim()
// MAGIC       inputCount = l.toLong
// MAGIC     }
// MAGIC     if(line.contains("output_count=")){
// MAGIC       var l = line.replace("output_count=", "")
// MAGIC       l = l.trim()
// MAGIC       outputCount = l.toLong
// MAGIC     }
// MAGIC   })
// MAGIC }
// MAGIC def ReadCDCArchiveView(log: String) {
// MAGIC   val contents = dbutils.fs.head(log)
// MAGIC   val fileSplit = contents.split("\n")
// MAGIC   fileSplit.foreach(line => {
// MAGIC     if(line.contains("record_count=")){
// MAGIC       var l = line.replace("record_count=", "")
// MAGIC       l = l.trim()
// MAGIC       inputCount = l.toLong
// MAGIC       outputCount = l.toLong
// MAGIC     }
// MAGIC     if(line.contains("partition_dt=")){
// MAGIC       var l = line.replace("partition_dt=", "")
// MAGIC       l = l.trim()
// MAGIC       viewMaxPartitionDt = l
// MAGIC     }
// MAGIC     if(line.contains("prev_dt_count=")){
// MAGIC       var l = line.replace("prev_dt_count=", "")
// MAGIC       l = l.trim()
// MAGIC       prevCount = l.toLong
// MAGIC     }
// MAGIC   })
// MAGIC }
// MAGIC 
// MAGIC def ReadViews(log: String) {
// MAGIC   val contents = dbutils.fs.head(log)
// MAGIC   val fileSplit = contents.split("\n")
// MAGIC   fileSplit.foreach(line => {
// MAGIC     if(line.contains("record_count=")){
// MAGIC       var l = line.replace("record_count=", "")
// MAGIC       l = l.trim()
// MAGIC       inputCount = l.toLong
// MAGIC       outputCount = l.toLong
// MAGIC     }
// MAGIC     if(line.contains("partition_dt=")){
// MAGIC       var l = line.replace("partition_dt=", "")
// MAGIC       l = l.trim()
// MAGIC       viewMaxPartitionDt = l
// MAGIC     }
// MAGIC   })
// MAGIC }
// MAGIC 
// MAGIC def VerifyLanding(): Boolean = {
// MAGIC   val copyCount = inputCount
// MAGIC   (copyCount == inputCount) && (copyCount == outputCount)
// MAGIC }
// MAGIC 
// MAGIC def VerifyFullView(): Boolean = {
// MAGIC   println(partitionDt)
// MAGIC   println(viewMaxPartitionDt)
// MAGIC   viewMaxPartitionDt == "true"
// MAGIC }
// MAGIC 
// MAGIC def VerifyCopy(): Boolean = {
// MAGIC   inputCount = dbutils.widgets.get("inputcount").toLong
// MAGIC   outputCount = dbutils.widgets.get("outputcount").toLong
// MAGIC   inputCount == outputCount
// MAGIC }
// MAGIC 
// MAGIC def VerifyCDCArchive(): Boolean = {
// MAGIC   println(partitionDt)
// MAGIC   println(viewMaxPartitionDt)
// MAGIC   println(prevCount)
// MAGIC   println(inputCount)
// MAGIC   ("true" == viewMaxPartitionDt) && (prevCount == inputCount)
// MAGIC }
// MAGIC 
// MAGIC def WriteToDb(activityStatus: String){
// MAGIC   val conn = DriverManager.getConnection(metaUrl + ";user="+metaUser+";password="+metaPass+";useUnicode=true;characterEncoding=UTF-8")
// MAGIC   val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
// MAGIC   val prep = conn.prepareStatement("INSERT INTO audit.activity_execution_log (pipeline_run_id,pipeline_name, activity_run_id, activity_name, activity_start_dt, activity_end_dt, activity_type, activity_status, table_name, input_count, output_count, resource_group_name, data_factory_name, last_updated_by, last_updated_ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
// MAGIC 
// MAGIC   val pipelineRunId = dbutils.widgets.get("pipelinerunid")
// MAGIC   val pipelineName = dbutils.widgets.get("pipelinename")
// MAGIC   val activityRunId = dbutils.widgets.get("activityrunid")
// MAGIC   val activityName = dbutils.widgets.get("activityname")
// MAGIC   val activityStartDate = dbutils.widgets.get("activitystart")
// MAGIC   val activityEndDate = dbutils.widgets.get("activityend")
// MAGIC   val activityType = dbutils.widgets.get("activitytype")
// MAGIC   val tableName = dbutils.widgets.get("tablename")
// MAGIC 
// MAGIC   val resourceGroupName = dbutils.widgets.get("resourcegroupname")
// MAGIC   val dataFactoryName = dbutils.widgets.get("datafactoryname")
// MAGIC 
// MAGIC   prep.setString(1,pipelineRunId) //pipeline_run_id
// MAGIC   prep.setString(2,pipelineName) //pipeline_name
// MAGIC   prep.setString(3,activityRunId) //activity_run_id
// MAGIC   prep.setString(4,activityName) //activity_name
// MAGIC   prep.setTimestamp(5,StringToTimeStamp(activityStartDate)) //activity_start_date
// MAGIC   prep.setTimestamp(6,StringToTimeStamp(activityEndDate)) //activity_end_date
// MAGIC   prep.setString(7,activityType) //activity_type
// MAGIC   prep.setString(8,activityStatus) //activity_status
// MAGIC   prep.setString(9,tableName) //table_name
// MAGIC   prep.setLong(10,inputCount) //input_count
// MAGIC   prep.setLong(11,outputCount) //output_count
// MAGIC   prep.setString(12,resourceGroupName) //resource_group_name
// MAGIC   prep.setString(13,dataFactoryName) //data_factory_name
// MAGIC   prep.setString(14,metaUser) //last_updated_by
// MAGIC   prep.setTimestamp(15,TimeStampNow()) //last_updated_ts
// MAGIC   val rs = prep.executeUpdate()
// MAGIC   conn.close()
// MAGIC }
// MAGIC 
// MAGIC if (verificationType.toLowerCase() == "landing" || verificationType.toLowerCase() == "delimit"){
// MAGIC   //val readLogPath = GetLogPath(verificationType, logPath)
// MAGIC   val yearmonth = partitionDt.take(6)
// MAGIC   val day = partitionDt.takeRight(2)
// MAGIC   val readLogPath = GetLogPath(verificationType, logPath, yearmonth, day)
// MAGIC    
// MAGIC   try
// MAGIC   {
// MAGIC     ReadCounts(readLogPath)
// MAGIC   } 
// MAGIC   catch
// MAGIC   {
// MAGIC     case _: Throwable => {
// MAGIC       throw new IllegalArgumentException("Failed to read log file. logPath=" + readLogPath);
// MAGIC       WriteToDb("Failed")
// MAGIC     }
// MAGIC   }
// MAGIC   
// MAGIC   if(VerifyLanding() && prevActivityStatus == "Succeeded"){
// MAGIC     WriteToDb("Succeeded")
// MAGIC   }else{
// MAGIC     WriteToDb("Failed")
// MAGIC     throw new IllegalArgumentException("Activity failed, verification failed.");
// MAGIC   }
// MAGIC }else if (verificationType.toLowerCase() == "copy"){
// MAGIC   if(VerifyCopy() && prevActivityStatus == "Succeeded"){
// MAGIC     WriteToDb("Succeeded")
// MAGIC   }else{
// MAGIC     WriteToDb("Failed")
// MAGIC     throw new IllegalArgumentException("Activity failed, verification failed.");
// MAGIC   }
// MAGIC }else if (verificationType.toLowerCase() == "staging" || verificationType.toLowerCase() == "active" || (verificationType.toLowerCase() == "archive" && loadType == "full") ){
// MAGIC   //val readLogPath = GetLogPath(verificationType, logPath)
// MAGIC   val yearmonth = partitionDt.take(6)
// MAGIC   val day = partitionDt.takeRight(2)
// MAGIC   val readLogPath = GetLogPath(verificationType, logPath, yearmonth, day)
// MAGIC    
// MAGIC   try
// MAGIC   {
// MAGIC     ReadViews(readLogPath)
// MAGIC   } 
// MAGIC   catch
// MAGIC   {
// MAGIC     case _: Throwable => {
// MAGIC       throw new IllegalArgumentException("Failed to read log file. logPath=" + readLogPath);
// MAGIC       WriteToDb("Failed")
// MAGIC     }
// MAGIC   }
// MAGIC   if(VerifyFullView() && prevActivityStatus == "Succeeded"){
// MAGIC     WriteToDb("Succeeded")
// MAGIC   }else{
// MAGIC     WriteToDb("Failed")
// MAGIC     throw new IllegalArgumentException("Activity failed, verification failed.");
// MAGIC   }
// MAGIC }else if (verificationType.toLowerCase() == "archive" &&  (loadType == "cdc" || loadType == "delta")){
// MAGIC   //val readLogPath = GetLogPath(verificationType, logPath)
// MAGIC   val yearmonth = partitionDt.take(6)
// MAGIC   val day = partitionDt.takeRight(2)
// MAGIC   val readLogPath = GetLogPath(verificationType, logPath, yearmonth, day)
// MAGIC    
// MAGIC   try
// MAGIC   {
// MAGIC     ReadCDCArchiveView(readLogPath)
// MAGIC   } 
// MAGIC   catch
// MAGIC   {
// MAGIC     case _: Throwable => {
// MAGIC       throw new IllegalArgumentException("Failed to read log file. logPath=" + readLogPath);
// MAGIC       WriteToDb("Failed")
// MAGIC     }
// MAGIC   }
// MAGIC   if(VerifyCDCArchive() && prevActivityStatus == "Succeeded"){
// MAGIC     WriteToDb("Succeeded")
// MAGIC   }else{
// MAGIC     WriteToDb("Failed")
// MAGIC     throw new IllegalArgumentException("Activity failed, verification failed.");
// MAGIC   }
// MAGIC }
// MAGIC else{
// MAGIC   throw new IllegalArgumentException("Verification("+verificationType+") and Load("+loadType+") type combination is not supported.");
// MAGIC }
