import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import scala.sys.process._
import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer



var logLines: ListBuffer[String] = new ListBuffer[String]()


def log(msg: String): Unit = logLines.append(msg)

def error(msg: String){
  throw new Exception(msg)
}

def writeLog(file: String, yearmonth: String, day: String){
  val outFile = file + yearmonth + "/" + day + "/" + "delimit_count_0.log"
  var outS = ""
  //logLines.value.asScala.foreach(x => outS += x + "\n")
  logLines.foreach(x => outS += x + "\n")
  println(outS)
  dbutils.fs.put(outFile, outS, true)
}

println("Creating new context")
 
    import spark.implicits._

println("Code starting for column modifications")
log("Code starting for column modifications")
	
	
	
	 val path = dbutils.widgets.get("path")
     val out_path = dbutils.widgets.get("out_path")
	 val col_value = dbutils.widgets.get("col_query")
	 val tname = dbutils.widgets.get("table_name")
     val logPath = dbutils.widgets.get("logpath")
     val adls_acc = dbutils.widgets.get("adls_acc")
     val output_del = dbutils.widgets.get("output_del")
     val input_del = dbutils.widgets.get("input_del")
     val input_path = adls_acc+"/"+path
	 val output_path = adls_acc+"/"+out_path

	println("input_path :: "+input_path)
    println("output_path :: "+output_path)
	println("col_value :: "+col_value) 
	println("table_name :: "+tname)
    println("logPath :: "+logPath)
     
    log("input_path ::"+input_path)
    log("output_path :: "+output_path)
	log("col_value :: "+col_value) 
	log("table_name :: "+tname)
    log("logPath :: "+logPath)

    dbutils.fs.rm(output_path,true)
    //val df = spark.read.option("quote", "\u0003").option("delimiter", input_del).option("header", "false").option("nullValue","").csv(input_path)
val df = spark.read
      .option("delimiter", input_del)
      .option("header", "false")
      .option("quote", "\u0003")
      .option("escape", "")
      .option("nullValue","")
      .csv(spark.read.textFile(input_path))

    val cnt = df.count.toInt
    log("input_count="+cnt.toString)
    if (cnt == 0) {
      val s = ""
      val t = sc.parallelize(s, 1)
      println("Empty File Output is created in the location ::" + output_path )
      log("Empty File Output is created in the location ::" + output_path )
      t.saveAsTextFile(output_path)
    } else {

    println("loading file ::"+input_path)
  
	df.createOrReplaceTempView(tname)

    println("Temporary view created")
	val df1 = spark.sql("select "+col_value+" from "+tname)
    df1.show()
      
      
      val df2 = df1.select(df1.columns.map(c => col(c).cast(StringType)): _ * )

log("output_count="+df2.count.toString)
df2.write.format("com.databricks.spark.csv").option("delimiter", output_del).option("quoteAll", "false").option("nullValue",null).option("emptyValue",null).option("ignoreLeadingWhiteSpace", "false").option("ignoreTrailingWhiteSpace", "false").mode(SaveMode.Overwrite).csv(output_path)
	println("Data Migration is Done")
    log("Data processing is done")
    }

println("============== FINISHED ==============")
println("============== FINISHED ==============")
val s = out_path.split("/").last
writeLog(logPath, s.take(6), s.takeRight(2))

println("============== FINISHED ==============")
println("============== FINISHED ==============")
