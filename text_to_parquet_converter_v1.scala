import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

var databaseName = ""
var tableName = ""
var partitionDate = ""
var partition = ""
var sourceName = ""
var repatition_count = ""
def params(){
  tableName = dbutils.widgets.get("tableName")
  databaseName = dbutils.widgets.get("databaseName")
  partitionDate = dbutils.widgets.get("partitionDate")
  partition = dbutils.widgets.get("partition")
  sourceName = dbutils.widgets.get("sourceName")
  repatition_count = dbutils.widgets.get("partition_file_size")
}

def test(){
  tableName = "pol_pty"
  databaseName = "archive"
  partitionDate = "201905@23"
  partition = "snapshot_year_month,snapshot_day"
  sourceName = "cesar"
}

def cleanup(d_name: String, t_name: String,path: String){
  try{
    sqlContext.sql("drop table parquet_"+d_name+".`"+t_name+"_parquet`").show()
    println("Droped the table")
  } 
  catch {
    case e:
      Exception => println("Exception caught" + e)
  }
  
  try{
    //dbutils.fs.rm(path,true)
    //println("Folder deleted")
  } 
  catch {
    case e:
      Exception => println("Exception caught" + e)
  }
}


params()
val partitions = partition.split(",")
var par = ""
val part = partitions.foreach(pt => {
  par = par+"`"+pt+"` string,"
})
par = par.stripPrefix(",").stripSuffix(",").trim


var df : DataFrame = null
var partition_path = ""
var partition_value = ""
var db_name = databaseName.split(",")
db_name.foreach(db => {
    sqlContext.sql("CREATE DATABASE IF NOT  EXISTS parquet_"+db+"_"+sourceName).show()
	val LOC_DF = sqlContext.sql("DESCRIBE FORMATTED "+db+"_"+sourceName+"."+tableName)
	val input_location = LOC_DF.filter(LOC_DF("col_name")==="Location").select("data_type").collect.mkString("").replaceAll("\\[","").replaceAll("\\]","").trim
	val input_location_parquet = input_location.replaceAll("/"+sourceName+"/", "/"+sourceName+"_parquet/")
	println("input_location_parquet : "+input_location_parquet)
  
	cleanup(db+"_"+sourceName,tableName,input_location_parquet)
	
	val old_schema = sqlContext.sql("show create table "+db+"_"+sourceName+"."+tableName)
	var old_schema_split = old_schema.collect.mkString(",").replaceAll("\\[","").replaceAll("\\]","").split("PARTITIONED BY ")
	var createddl_parquet = old_schema_split(0).replaceAll("CREATE EXTERNAL TABLE `"+db+"_"+sourceName+"`.`"+tableName+"`","CREATE TABLE IF NOT EXISTS `parquet_"+db+"_"+sourceName+"`.`"+tableName+"_parquet`")
	
	createddl_parquet = createddl_parquet.replaceAll("\\n", "")+" PARTITIONED BY ("+par+") STORED AS PARQUET LOCATION  '"+input_location_parquet+"' ".replaceAll("\\n", "")
	println("DDL created : "+createddl_parquet)
	
	sqlContext.sql(createddl_parquet)
    println("table created")
	if (partitionDate == "ALL") {
		
		println("ALL"+partition)
				
		val x = sqlContext.sql("show partitions "+db+"_"+sourceName+"."+tableName).collect.foreach(pt => {
            println("pt is "+pt)
			
			partition_path = pt.mkString("").replaceAll("\\[","").replaceAll("\\]","")
            println("partition_path is "+partition_path)
			partition_value = partition_path.split("/").mkString(" and ")
            println("partition_value is "+partition_value)
			df = sqlContext.sql("select * from "+db+"_"+sourceName+"."+tableName+" where "+partition_value )
          //df.show(10,false)
           // println("df count is :"+df.count)
          //df.printSchema()
			println(input_location_parquet+"/"+partition_path)
			try {
				dbutils.fs.rm(input_location_parquet+"/"+partition_path,true)
			} 
			catch {
				case e:
					Exception => println("Exception caught" + e)
			}
			df.repartition(repatition_count.toInt).write.parquet(input_location_parquet+"/"+partition_path)
          println("writing done")
		})
		sqlContext.sql("msck repair table parquet_"+db+"_"+sourceName+"."+tableName+"_parquet")
	}
	else {
		val custom_pt = partitionDate.split(",")
		
		custom_pt.foreach(pt => {
			var cust_pt_condition = ""	
			var cust_path = ""
			var pt_value = pt.split("@")
			for (i <- 0 to pt_value.size - 1){
				
				cust_pt_condition = cust_pt_condition+partitions(i)+"='"+pt_value(i)+"' and "
				cust_path = cust_path+partitions(i)+"="+pt_value(i)+"/"
			}
      println("cust_pt_condition: " + cust_pt_condition )
      println("cust_path: " + cust_path )
			cust_pt_condition = cust_pt_condition.stripSuffix(" and ").trim
			cust_path = cust_path.stripSuffix("/").trim
			df = sqlContext.sql("select * from "+db+"_"+sourceName+"."+tableName+" where "+cust_pt_condition)
			try{
			dbutils.fs.rm(input_location_parquet+"/"+cust_path,true)
			} 
			catch {
				case e:
				Exception => println("Exception caught" + e)
			}
      df.repartition(repatition_count.toInt).write.parquet(input_location_parquet+"/"+cust_path)
			
		})
		sqlContext.sql("msck repair table parquet_"+db+"_"+sourceName+"."+tableName+"_parquet")
	}
})