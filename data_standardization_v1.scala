import java.util
import org.apache.commons.lang.StringEscapeUtils

object Constants extends Serializable {

  val EMPTY: String = ""
  val SPACE: String = " "
  val STRING_NULL: String = "null"

  val DEFAULT_REPLACE_REGEX_PATTERN: String = "[^\\u0020-\\u00FF\\u0001\\t\\n]"

  val ONE: Int = 1
  val ZERO: Int = 0

  
  val TAB_CHAR: String = "\\t"
  val PATTERN_SPACES: String = "[\\s]+"

  val NONE: String = "none"

  val NULL: String = ""
  val STR_ZERO: String = "0"
  val STR_ONE: String = "1"
  val FALSE: String = "false"
  val TRUE: String = "true"

  val HASH_DELIMITER: String = "#"
  val DOUBLE_COLON_DELIMITER: String = "::"
  val EQUAL_DELIMITER: String = "="
  val COMMA_DELIMITER: String = ","
  val PIPE_DELIMITER: String = "|"
  val CTRL_A_DELIMITER: String = "\u0001"

  val DEFAULT_DELIMITERS: util.List[String] = util.Arrays.asList(PIPE_DELIMITER,
    COMMA_DELIMITER,
    CTRL_A_DELIMITER,
    DOUBLE_COLON_DELIMITER,
    EQUAL_DELIMITER,
    HASH_DELIMITER)

  val REGEX_COMMENT_STRING: String = "^#[\\s\\S]+"

  val DELIMITER_NAME: String = "separator"

  val INPUT_DELIMITER_NAME: String = "input_separator"
  val OUTPUT_DELIMITER_NAME: String = "output_separator"
  val REPLACE_REGEX_PATTERN: String = "replace_regex_pattern"

  val IN_DATE_FORMAT: String = "in_date_format"
  val IN_TIME_FORMAT: String = "in_time_format"
  val FORMAT_TIME_FLAG: String = "format_time_flag"
  val TRIM_TEXT_FLAG: String = "trim_text_flag"
  val FORMAT_DATE_FLAG: String = "format_date_flag"
  val CLEAN_TEXT_FLAG: String = "clean_text_flag"

  val LOCAL_MODE: String = "local"
  val MR_MODE: String = "mr"

  val INVALID_DATE: String = "9999-09-09"
  val INVALID_DATE2: String = "9999-09-08"

  val PIG_DATE_FORMAT: String = "yyyy-MM-dd"
  val DEFAULT_PIG_DATE_FORMAT: String = "yyyyMMdd"
  val YYYY: String = "yyyy"
  val DATE_FORMAT_LIST: util.List[String] = util.Arrays.asList("MM-dd-yyyy",
    "dd-MM-yyyy",
    "yyyy-MM-dd",
    "MM/dd/yyyy",
    "dd/MM/yyyy",
    "yyyy/MM/dd",
    "MM.dd.yyyy",
    "dd.MM.yyyy",
    "yyyy.MM.dd",
    "MMddyyyy",
    "ddMMyyyy",
    "yyyyMMdd")

  val PIG_TIMESTAMP_FORMAT: String = "yyyy-MM-dd HH:mm:ss.sss"
  val INVALID_TIMESTAMP: String = "9999-09-09 09:09:09"
  val TIME_FORMAT_LIST: util.List[String] = util.Arrays.asList(
    "MM-dd-yyyy HH:mm:ss",
    "MM-dd-yyyy HH:mm:ss:SSS",
    "dd-MM-yyyy HH:mm:ss",
    "dd-MM-yyyy HH:mm:ss:SSS",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HHmmssSSS",
    "MM/dd/yyyy HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss:SSS",
    "dd/MM/yyyy HH:mm:ss",
    "dd/MM/yyyy HH:mm:ss:SSS",
    "yyyy/MM/dd HH:mm:ss",
    "yyyy/MM/dd HH:mm:ss:SSS",
    "MM.dd.yyyy HH:mm:ss",
    "MM.dd.yyyy HH:mm:ss:SSS",
    "dd.MM.yyyy HH:mm:ss",
    "dd.MM.yyyy HH:mm:ss:SSS",
    "yyyy.MM.dd HH:mm:ss",
    "yyyy.MM.dd HH:mm:ss:SSS",
    "yyyyMMddHHmmss",
    "MM/dd/yyyy hh:mm:ss aaa"
  )

  val PATH_SEPARATOR: String = "/"
  val DOT: String = "."
  val REPLACE_DOTS: String = "[\\.]+"
  val PATTERN_CLEAN_FILENAME: String = "[^a-zA-Z0-9\\._]+"
  val REPLACE_UNDER_SCORE: String = "[_]+"
  val UNDER_SCORE: String = "_"
  val BAD_RECORD: String = "bad_record>>"
  val YYYY_MM_DD_HH_MM_SS: String = "yyyy-MM-dd HH:mm:ss"
  val ETL_BATCH_RUNDATE: String = "etl_batch_rundate"
  val REC_DEL: String = "rec_del"
  val _99: String = "-99"
  val ETL_BATCH_ID: String = "etl_batch_id"
}


import java.math.BigDecimal
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.util.CollectionAccumulator
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import org.apache.commons.lang.StringEscapeUtils

var logLines: ListBuffer[String] = new ListBuffer[String]()


def log(msg: String): Unit = logLines.append(msg)

def error(msg: String){
  throw new Exception(msg)
}

def writeLog(file: String, yearmonth: String, day: String){
  val outFile = file + yearmonth + "/" + day + "/" + "landing_count_0.log"
  var outS = ""
  logLines.foreach(x => outS += x + "\n")
  println(outS)
  dbutils.fs.put(outFile, outS, true)
}

def warning(msg: String){
  log("*WARNING* " + msg)
}

val CleanText = (s: AnyRef, regex: String) => {
  if(s == null || s == ""){
    null
  }else{
    try{
      val out = s.toString.replaceAll(regex, Constants.EMPTY)
      if(out == ""){
        null
      }else{
        out
      }
    } catch {
      case e: Exception => {
        println("CleanText - " + e + " - " + s)
        s.toString
      }
    }
  }
}

val TrimText = (s: AnyRef) => {
  if(s == null || s == ""){
    null
  }else{
    try{
      val out = s.toString.trim()
      if(out == ""){
        null
      }else{
        out
      }
    } catch {
      case e: Exception => {
        println("TrimText - " + e + " - " + s)
        s.toString
      }
    }
  }
}

val Format2Boolean = (input: AnyRef) => {
  val column = input.toString.trim()
  if(column.isEmpty()){
    column
  }else{
    try column.toLowerCase() match {
      case Constants.STR_ZERO => Constants.FALSE
      case Constants.STR_ONE => Constants.TRUE
      case null => null
      case _ => column
    } catch {
      case e: Exception => {
        println("Format2Bool - " + e + " - " + input)
        input.toString
      }
    }
  }
}

val Format2Int = (input: AnyRef) => {
  if(input == null){
    null
  }else{
    var column = input.toString.trim()
    column = column.replaceAll(Constants.COMMA_DELIMITER, Constants.EMPTY).trim()
    if(column.isEmpty()){
      column
    }else{
      try {
        val big: BigDecimal = new BigDecimal(column)
        big.toString
      } catch {
        case e: Exception => {
          println("Format2Int - " + e + " - " + input)
          input.toString
        }
      }
    }
  } 
}

  val Format2Date = (input: AnyRef, inDateFormat: String, outDateFormat: String) => {
    var column = ""
    try{
      column = input.toString.trim()
    } catch {
      case e: Exception => {
        println("Format2Date - " + e + " - " + input)
      }
    }
    var inFormatString = inDateFormat.trim()
    var outFormatString = outDateFormat.trim()
    if(inFormatString.isEmpty()){
      inFormatString = Constants.DEFAULT_PIG_DATE_FORMAT
    }
    if(outFormatString.isEmpty()){
      outFormatString = Constants.PIG_DATE_FORMAT
    }
    if(column.isEmpty()){
      column
    }else{
      val inFormat: SimpleDateFormat = new SimpleDateFormat(inFormatString)
      val outFormat: SimpleDateFormat = new SimpleDateFormat(outFormatString)
      val inDate: String = column
      if (!Constants.DATE_FORMAT_LIST.contains(inFormat.toPattern)) {
            log(
                "_INFO: Couldn't parse to date format. Input:" + inDate +
                    " but expected format is:" +
                    Constants.PIG_DATE_FORMAT)
            Constants.INVALID_DATE2
      }
      val inDateWithoutZero: String = inDate.replace("0", "")
      val expectedLength: Int = inDate.length - inDateWithoutZero.length
       if (!inFormatString.contains(Constants.YYYY) || expectedLength > 6) {
             //log("_INFO: Couldn't parse to date format " + inDate)
       }
       if (!outFormatString.contains(Constants.YYYY) || expectedLength > 6) {
             //log("_INFO: Couldn't parse to date format " + inDate)
       }
      try {
        val dt: Date = inFormat.parse(inDate)
        outFormat.format(dt)
      } catch {
        case e: Exception => {
          println("Format2Date - " + e + " - " + input)
          column
        }
      }
    }
  }

val Format2Time = (input: AnyRef, inTSFormat: String, outTSFormat: String) => {
  var column = input.toString.trim()
  var inFormatString = inTSFormat
  var outFormatString = outTSFormat
  if(inFormatString.isEmpty()){
    inFormatString = Constants.PIG_TIMESTAMP_FORMAT
  }
  if(outFormatString.isEmpty()){
    outFormatString = Constants.PIG_TIMESTAMP_FORMAT
  }
  if(column.isEmpty()){
    column
  }else{
    val inFormat: SimpleDateFormat = new SimpleDateFormat(inFormatString)
    val outFormat: SimpleDateFormat = new SimpleDateFormat(outFormatString)
    val inDate: String = column
    if (!Constants.TIME_FORMAT_LIST.contains(inFormat.toPattern)) {
      println(
          "_INFO: Couldn't parse to date format. Input:" + inDate +
              " but expected format is:" +
              Constants.PIG_DATE_FORMAT)
      Constants.INVALID_DATE2
    }
    try {
      val dt: Date = inFormat.parse(inDate)
      outFormat.format(dt)
    } catch {
      case e: Exception => {
        println("Format2Time - " + e + " - " + input)
        column
      }
    }
  }
}

sqlContext.udf.register("CleanText", CleanText)
sqlContext.udf.register("TrimText", TrimText)
sqlContext.udf.register("Format2Boolean", Format2Boolean)
sqlContext.udf.register("Format2Int", Format2Int)
sqlContext.udf.register("Format2Date", Format2Date)
sqlContext.udf.register("Format2Time", Format2Time)


import scala.collection.mutable.Map
import java.util
import scala.beans.BeanProperty
//import org.apache.commons.lang3.StringEscapeUtils
import scala.collection.mutable.LinkedHashMap

object DataType extends Enumeration{
  
  val INT: DataType = new DataType("int", 1)
  val DOUBLE: DataType = new DataType("double", 2)
  val STRING: DataType = new DataType("string", 3)
  val TIMESTAMP: DataType = new DataType("timestamp", 4)
  val DATE: DataType = new DataType("date", 5)
  val BOOLEAN: DataType = new DataType("boolean", 6)
  val BIGINT: DataType = new DataType("bigint", 7)
  val DECIMAL: DataType = new DataType("decimal", 8)

  class DataType(@BeanProperty var name: String, @BeanProperty var ids: Int) extends Val {}
  
  val fieldsMap: util.Map[String, DataType] = new util.HashMap[String, DataType]()
  val revfieldsMap: util.Map[Integer, DataType] = new util.HashMap[Integer, DataType]()

  def getDataTypeName(name: String): DataType =fieldsMap.get(name.toLowerCase())

  def getDataTypeName(id: Int): DataType = revfieldsMap.get(id)
}

class CustomProperties(var map: LinkedHashMap[String,String]) extends Serializable {
  val oldMap = map.clone
  //val columns: LinkedHashMap[String,DataType.DataType] = new LinkedHashMap()
  val columns: LinkedHashMap[String,(DataType.DataType, String)] = new LinkedHashMap()
  var regex: String = GetValue(Constants.REPLACE_REGEX_PATTERN)
  var inDate: String = GetValue(Constants.IN_DATE_FORMAT)
  var inTime: String = GetValue(Constants.IN_TIME_FORMAT)
  var formatDate: Boolean = GetValue(Constants.FORMAT_DATE_FLAG).toBoolean
  var formatTime: Boolean = GetValue(Constants.FORMAT_TIME_FLAG).toBoolean
  var cleanText: Boolean = GetValue(Constants.CLEAN_TEXT_FLAG).toBoolean
  var trimText: Boolean = GetValue(Constants.TRIM_TEXT_FLAG).toBoolean
  var inputSeperator: String = GetValue(Constants.INPUT_DELIMITER_NAME)
  var outputSeperator: String = GetValue(Constants.OUTPUT_DELIMITER_NAME)
  map.foreach({case (key, value) =>
    var inputDataType = value
    var precisionScale = ""
    if(value.toLowerCase().startsWith("decimal(")) {
      inputDataType = "decimal"
      precisionScale = value.substring("decimal".length, value.length)  
    }
    
    val colType = DataType.getDataTypeName(inputDataType)
    if (colType == null){
      error("Column type not found - " + key + ":" + value)
    }else{
           columns.put(key, (DataType.getDataTypeName(inputDataType), precisionScale))
    }
  })
  
  def printLogger(): Unit = {
    log("#################################")
    log(
            "property file is loaded successfully.  No of entries:" +
                oldMap.size)
    log(
        "Input column delimiter(printable):" + StringEscapeUtils.escapeJava(
            inputSeperator))
    log(
        "Output column delimiter(printable): " +
            StringEscapeUtils.escapeJava(outputSeperator))
    log(
        "Input column delimiter(non-printable):" + inputSeperator)
    log(
        "Output column delimiter(non-printable): " + outputSeperator)
    log("clean_text_flag: " + cleanText)
    log("replace_regex_pattern: " + regex)
    val pattern = oldMap.get(Constants.REPLACE_REGEX_PATTERN)
    if (pattern == null || pattern.isEmpty)
        log(
            "Property \"" + Constants.REPLACE_REGEX_PATTERN +
                "\" value is not defined or empty in property file. Note: default regex pattern will be applied as \"" +
                regex +
                "\"")
    log("in_time_format_flag: " + formatTime)
    log("in_date_format_flag: " + formatDate)
    log("trim_text_flag: " + trimText)
    log("#################################")
  }

  def GetValue(key: String): String = {
    if(map.contains(key)){
      val value = map(key)
      map.remove(key)
      value
    }else{
      if(key == Constants.REPLACE_REGEX_PATTERN){
        Constants.DEFAULT_REPLACE_REGEX_PATTERN
      }else{
        error(key + " does not exist in the property file.")
        null
      }
    }
  }
}

def init(){
    for (field <- DataType.values) {
      val value = field.asInstanceOf[DataType.DataType]
      DataType.fieldsMap.put(value.getName, value)
      DataType.revfieldsMap.put(value.getIds, value)
  }
}

def addUdfWrapper(s: String, wrapper: String, additionalArgs: scala.collection.mutable.ListBuffer[String]): String ={
  var paramS = " "
  additionalArgs.foreach(param => {
    paramS += ",'"
    paramS += param
    paramS += "'"
  })
  wrapper + "(" + s + paramS + ")"
}

def addNaming(s: String, name: String): String ={
  s + " as `" + name + "`"
  //s + " as " + name
}

def addCasting(s:String, typeValue: String): String = {
  "cast(" + s + " as " + typeValue + ")"
}


import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.split
import scala.collection.mutable.LinkedHashMap
import org.apache.spark.util.CollectionAccumulator
import java.util.Properties
import java.io.StringReader
import org.apache.commons.lang.StringEscapeUtils

val map: LinkedHashMap[String, String] = new LinkedHashMap()
logLines = new ListBuffer[String]()

//unique for each table
var tempView = ""
//mnt location
var inputLoc = ""
//mnt location
var outputLoc = ""
//file
var inputType = ""
//file or parquet
var outputType = ""
//mnt location
var badRecordLoc = ""
//comma deliminated header string
var headerStr = ""
var header: Array[String] = null
var hasHeaderInput = "false"
var hasHeaderOutput = "false"
var batchId = ""
var inputPattern = ""
var recDel = ""
var logPath = ""
//true or false
var multiCharDelim: String = ""
var compression = ""
var propertyFile = ""



def params(){
  tempView = dbutils.widgets.get("tempView")
  inputLoc = dbutils.widgets.get("inputLoc")
  outputLoc = dbutils.widgets.get("outputLoc")
  badRecordLoc = dbutils.widgets.get("badRecordLoc")
  inputType = dbutils.widgets.get("inputType")
  outputType = dbutils.widgets.get("outputType")
  hasHeaderInput = dbutils.widgets.get("hasHeaderInput")
  hasHeaderOutput = dbutils.widgets.get("hasHeaderOutput")
  batchId = dbutils.widgets.get("batchId")
  recDel = dbutils.widgets.get("recDel")
  inputPattern = dbutils.widgets.get("inputPattern")
  headerStr = dbutils.widgets.get("headerStr").trim()
  logPath = dbutils.widgets.get("logpath")
  multiCharDelim = dbutils.widgets.get("multiCharDelim")
  compression = dbutils.widgets.get("compression")
  propertyFile = dbutils.widgets.get("propertyFile")
  
  val contents = dbutils.fs.head(propertyFile)
  val lineContents = contents.split("\r\n")
  lineContents.foreach(line => {
     val lineSplit = line.split("=")
     map.put(lineSplit(0), lineSplit(1).replaceAll("\\n", ""))
  })
}
println("============== Starting ==============")
init()
//testVals4()
params()
var repatition_count = "50"


try{
  repatition_count = dbutils.widgets.get("repatition_count")
  
  
}
catch{
  case e:
    Exception => println("Exception caught : " + e)
}

//sqlContext.sql("set spark.sql.shuffle.partitions="+repatition_count).show()
println("============== Params Done ==============")
val props = new CustomProperties(map)
var dfInput: org.apache.spark.sql.DataFrame = null
if(headerStr.isEmpty){
  header = props.columns.keys.toArray
}else{
  header = headerStr.split(",")
}
println("============== Props Done ==============")
inputType match {
  case "FILE" => {
    if (multiCharDelim == "true"){
      val sep = props.inputSeperator
      var schema = (new StructType)
      header.foreach({
        col => schema = schema.add(col, StringType)
      })
      dfInput = spark.read
      .option("delimiter", "\u0001")
      .option("header", hasHeaderInput)
      .option("quote", "\u0003")
      .option("escape", "")
      .option("nullValue","")
      .option("badRecordsPath", badRecordLoc)
      .schema(schema)
      .csv(spark.read.textFile(inputLoc)
          .map(line => line.replaceAll(sep, "\u0001").replaceAll("\u0003","")))
    }else{
      println("============== input - pre schema ==============")
      var schema = (new StructType)
      header.foreach({
        col => schema = schema.add(col, StringType)
      })
      println("============== input - post schema ==============")
      dfInput = spark.read
      .option("delimiter", StringEscapeUtils.unescapeJava(props.inputSeperator))
      .option("header", hasHeaderInput)
      .option("quote", "\u0003")
      .option("nullValue","")
      .option("escape", "")
      .option("badRecordsPath", badRecordLoc)
      .schema(schema)
      .csv(spark.read.textFile(inputLoc)
          .map(line => line.replaceAll("\u0003","")))
      println("============== input - post dataframe ==============")
    }
  }
  case _ => error("Input type is not supported. Input type: " + inputType)
}
println("============== Input Done ==============")
dfInput.createOrReplaceTempView(tempView)
var query = "select "
if(inputPattern.toLowerCase == "full_snapshot" || inputPattern.toLowerCase == "delta"){
  query += "'"+batchId+"' as ETL_BatchId,cast(from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as timestamp) as ETL_RunDate, cast("+recDel+" as string) as rec_del, "
  //query += "'"+batchId+"' as ETL_BatchId,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as ETL_RunDate, '"+recDel+"' as rec_del, "
}
if(inputPattern.toLowerCase == "cdc"){
  query += "'"+batchId+"' as ETL_BatchId,cast(from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as timestamp) as ETL_RunDate, "
}

var columns: ListBuffer[String] = new ListBuffer[String]()
props.columns.foreach({
  case (columnName, tplDataType) => 
    var tempCol = "`" + columnName + "`"
    //var tempCol = columnName
    val dataType = tplDataType._1
    val precisionScale = tplDataType._2

    var params: ListBuffer[String] = null
    if(props.cleanText){
      params = new ListBuffer[String]()
      params.append(props.regex)
      tempCol = addUdfWrapper(tempCol, "CleanText", params)
    }
    dataType match {
      case DataType.STRING => {
        if(props.trimText){
          params = new ListBuffer[String]()
          tempCol = addUdfWrapper(tempCol, "TrimText", params)
          tempCol = addCasting(tempCol, dataType.name)
        }
      }
      case DataType.INT => {
        params = new ListBuffer[String]()
        tempCol = addUdfWrapper(tempCol, "Format2Int", params)
        tempCol = addCasting(tempCol, dataType.name)
      }
      case DataType.BIGINT => {
        params = new ListBuffer[String]()
        tempCol = addUdfWrapper(tempCol, "Format2Int", params)
        tempCol = addCasting(tempCol, dataType.name)
      }
      case DataType.DOUBLE => {
        params = new ListBuffer[String]()
        tempCol = addUdfWrapper(tempCol, "Format2Int", params)
        tempCol = addCasting(tempCol, dataType.name)
      }
      case DataType.DECIMAL => {
        params = new ListBuffer[String]()
        tempCol = addUdfWrapper(tempCol, "Format2Int", params)
        val outDtType = dataType.name + precisionScale
        tempCol = addCasting(tempCol, outDtType)
      }
      case DataType.BOOLEAN => {
        params = new ListBuffer[String]()
        tempCol = addUdfWrapper(tempCol, "Format2Boolean", params)
        tempCol = addCasting(tempCol, dataType.name)
      }
      case DataType.DATE => {
        if(props.formatDate){
          params = new ListBuffer[String]()
          params.append(props.inDate)
          params.append("")
          tempCol = addUdfWrapper(tempCol, "Format2Date", params)
          tempCol = addCasting(tempCol, dataType.name)
        }
      }
      case DataType.TIMESTAMP => {
        if(props.formatTime){
          params = new ListBuffer[String]()
          params.append(props.inTime)
          params.append("")
          tempCol = addUdfWrapper(tempCol, "Format2Time", params)
          tempCol = addCasting(tempCol, dataType.name)
        }
      }
      case _ => error("Wrapper doesn't suppose this datatype. Input type: " + dataType)
    }
    
    tempCol = addNaming(tempCol, columnName)
    columns.append(tempCol)
})
query += columns.mkString(",")
query += " from " + tempView
println(query)
val dfFinal = sqlContext.sql(query)
props.printLogger()
println("============== Query Done ==============")
try { 
  dbutils.fs.ls(outputLoc)
  
  log(outputLoc + " location exists! Overwriting")
} catch {
  case e: Exception => log(outputLoc + " location does not exist!")
}
outputType match {
  case "PARQUET" => {
    println("============== Output Start ==============")
    if(compression.isEmpty || compression == "none"){
      sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    }else{
      sqlContext.setConf("spark.sql.parquet.compression.codec", compression)
    }
    val dfClean = dfFinal.na.fill("", dfFinal.columns)    
    dfClean.repartition(repatition_count.toInt).write.mode(SaveMode.Overwrite).parquet(outputLoc) 
    val dfFinalCount = spark.read.parquet(outputLoc)
    val sep = props.inputSeperator
    var dfInputC = spark.read
      .option("delimiter", "\u0001")
      .option("header", hasHeaderInput)
      .option("quote", "\u0003")
      .option("escape", "")
      .option("nullValue","")
      .csv(spark.read.textFile(inputLoc)
          .map(line => line.replaceAll(sep, "\u0001").replaceAll("\u0003","")))
    log("input_count="+dfInputC.count.toString)
    log("output_count="+dfFinalCount.count.toString)
  }
  case "TEXT" => {
    println("============== Output Start ==============")
    if(compression.isEmpty){
      compression = "none"
    }
    //display(dfFinal)
    //dfFinal.write.mode(SaveMode.Overwrite)
    //.option("header", hasHeaderOutput)
    //.option("sep", StringEscapeUtils.unescapeJava(props.outputSeperator))
    //.option("codec", compression)
    //.option("quoteAll", "false")
    //.option("nullValue",null)
    //.option("emptyValue",null)
    //.option("ignoreLeadingWhiteSpace", "false")
    //.option("ignoreTrailingWhiteSpace", "false")
    //.csv(outputLoc)
    
    dbutils.fs.rm (outputLoc, recurse = true)
    
    val dfClean = dfFinal.na.fill("", dfFinal.columns) //convert all nulls to empty
    val opdelimeter = StringEscapeUtils.unescapeJava(props.outputSeperator)
    val dfOutput = dfClean.map(n => n.mkString(opdelimeter)) //convert columns to single column in order to use TextWriter
    dfOutput.repartition(repatition_count.toInt).write.format("text")
    .option("mode", "overwrite") //not working, so cleanup manullay.
    .option("codec", "none")
    .option("path", outputLoc)
    .save()
    
    println("============== Output Wrote ==============")
    try { 
      val dfFinalCount = spark.read.option("sep", StringEscapeUtils.unescapeJava(props.outputSeperator)).option("header", hasHeaderOutput).csv(outputLoc)
      log("output_count="+dfFinalCount.count.toString)
      dbutils.fs.ls(outputLoc)
    } catch {
      case e: Exception => log("output_count=0")
    }
    
    val sep = props.inputSeperator
    var dfInputC = spark.read
      .option("delimiter", "\u0001")
      .option("header", hasHeaderInput)
      .option("quote", "\u0003")
      .option("escape", "")
      .option("nullValue","")
      .csv(spark.read.textFile(inputLoc)
          .map(line => line.replaceAll(sep, "\u0001").replaceAll("\u0003","")))
    log("input_count="+dfInputC.count.toString)
    
  }
  case _ => error("Output type is not supported. Output type: " + outputType)
}
println("============== Output Done ==============")
var maxYearMonth = ""
var maxDay = ""
outputLoc.split("/").foreach(line => {
  if(line.contains("snapshot_year_month")){
    maxYearMonth = line.replace("snapshot_year_month=", "")
  }
  if(line.contains("snapshot_day")){
    maxDay = line.replace("snapshot_day=", "")
  }
})

writeLog(logPath, maxYearMonth, maxDay)

println("============== FINISHED ==============")
println("============== FINISHED ==============")

// COMMAND ----------

import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import java.util.Calendar
import java.util.TimeZone
import java.text.SimpleDateFormat
import java.time.ZonedDateTime
import java.time.Instant
import java.nio.file.Paths
import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.http.util.EntityUtils


println("============== VALIDATION STARTED ==============")
println("============== VALIDATION STARTED ==============")

Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
var activityRunId = ""
var activityRunStart: Any = ""
val metaUrl = dbutils.widgets.get("metaurl")
val metaUser = dbutils.widgets.get("metauser")
//dev-secrets,sqldb-password
val metaPass = dbutils.secrets.get(dbutils.widgets.get("secretvault"),dbutils.widgets.get("metapass"))
val verificationType = dbutils.widgets.get("verificationtype")
val loadType = dbutils.widgets.get("inputPattern")
//val logPath = dbutils.widgets.get("logpath")
//val prevActivityStatus = dbutils.widgets.get("activitystatus")
val partitionDt = dbutils.widgets.get("partitiondt")

var inputCount = 0L
var outputCount = 0L
var viewMaxPartitionDt = ""
var prevCount = 0L

def jsonStrToMap(jsonStr: String): Map[String, Any] = {
  implicit val formats = org.json4s.DefaultFormats
  parse(jsonStr).extract[Map[String, Any]]
}

def GetLogPath(verify: String, path: String, yearmonth: String, day: String): String = {
   val count = 0
   val out = Paths.get(path, yearmonth, day, verify + "_count_" + count + ".log").toString().replaceAll("^wasb:/", "wasb://")
   println("Log file path:" + out)
   out
 }

def TimeStampNow(): Timestamp = {
  val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val today = Calendar.getInstance().getTime
  val now = timeFormat.format(today)
  val ts = java.sql.Timestamp.valueOf(now)
  ts
}

def StringToTimeStamp(str: String): Timestamp = {
  val zdt = Instant.parse(str)
  val ts = java.sql.Timestamp.from(zdt)
  ts
}

def ReadCounts(log: String) {
  val contents = dbutils.fs.head(log)
  val fileSplit = contents.split("\n")
  fileSplit.foreach(line => {
    if(line.contains("input_count=")){
      var l = line.replace("input_count=", "")
      l = l.trim()
      inputCount = l.toLong
    }
    if(line.contains("output_count=")){
      var l = line.replace("output_count=", "")
      l = l.trim()
      outputCount = l.toLong
    }
  })
}
def ReadCDCArchiveView(log: String) {
  val contents = dbutils.fs.head(log)
  val fileSplit = contents.split("\n")
  fileSplit.foreach(line => {
    if(line.contains("record_count=")){
      var l = line.replace("record_count=", "")
      l = l.trim()
      inputCount = l.toLong
      outputCount = l.toLong
    }
    if(line.contains("partition_dt=")){
      var l = line.replace("partition_dt=", "")
      l = l.trim()
      viewMaxPartitionDt = l
    }
    if(line.contains("prev_dt_count=")){
      var l = line.replace("prev_dt_count=", "")
      l = l.trim()
      prevCount = l.toLong
    }
  })
}

def ReadViews(log: String) {
  val contents = dbutils.fs.head(log)
  val fileSplit = contents.split("\n")
  fileSplit.foreach(line => {
    if(line.contains("record_count=")){
      var l = line.replace("record_count=", "")
      l = l.trim()
      inputCount = l.toLong
      outputCount = l.toLong
    }
    if(line.contains("partition_dt=")){
      var l = line.replace("partition_dt=", "")
      l = l.trim()
      viewMaxPartitionDt = l
    }
  })
}

def VerifyLanding(): Boolean = {
  val copyCount = inputCount
  (copyCount == inputCount) && (copyCount == outputCount)
}

def VerifyFullView(): Boolean = {
  println(partitionDt)
  println(viewMaxPartitionDt)
  viewMaxPartitionDt == "true"
}

def VerifyCopy(): Boolean = {
  inputCount = dbutils.widgets.get("inputcount").toLong
  outputCount = dbutils.widgets.get("outputcount").toLong
  inputCount == outputCount
}

def VerifyCDCArchive(): Boolean = {
  println(partitionDt)
  println(viewMaxPartitionDt)
  println(prevCount)
  println(inputCount)
  ("true" == viewMaxPartitionDt) && (prevCount == inputCount)
}

def WriteToDb(activityStatus: String, activityRunId: String, activityStartDate: Any){
  val conn = DriverManager.getConnection(metaUrl + ";user="+metaUser+";password="+metaPass+";useUnicode=true;characterEncoding=UTF-8")
  val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
  val prep = conn.prepareStatement("INSERT INTO audit.activity_execution_log (pipeline_run_id,pipeline_name, activity_run_id, activity_name, activity_start_dt, activity_end_dt, activity_type, activity_status, table_name, input_count, output_count, resource_group_name, data_factory_name, last_updated_by, last_updated_ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

  val pipelineRunId = dbutils.widgets.get("pipelinerunid")
  val pipelineName = dbutils.widgets.get("pipelinename")
  //val activityRunId = dbutils.widgets.get("activityrunid")
  val activityName = dbutils.widgets.get("activityname")
  //val activityStartDate = dbutils.widgets.get("activitystart")
  //val activityEndDate = dbutils.widgets.get("activityend")
  val activityType = dbutils.widgets.get("activitytype")
  val tableName = dbutils.widgets.get("tablename")

  

  prep.setString(1,pipelineRunId) //pipeline_run_id
  prep.setString(2,pipelineName) //pipeline_name
  prep.setString(3,activityRunId) //activity_run_id
  prep.setString(4,activityName) //activity_name
  prep.setTimestamp(5,StringToTimeStamp(activityStartDate.toString())) //activity_start_date
  prep.setTimestamp(6,TimeStampNow) //activity_end_date
  prep.setString(7,activityType) //activity_type
  prep.setString(8,activityStatus) //activity_status
  prep.setString(9,tableName) //table_name
  prep.setLong(10,inputCount) //input_count
  prep.setLong(11,outputCount) //output_count
  prep.setString(12,resourceGroupName) //resource_group_name
  prep.setString(13,dataFactoryName) //data_factory_name
  prep.setString(14,metaUser) //last_updated_by
  prep.setTimestamp(15,TimeStampNow()) //last_updated_ts
  val rs = prep.executeUpdate()
  conn.close()
}

val resourceGroupName = dbutils.widgets.get("resourcegroupname")
val dataFactoryName = dbutils.widgets.get("datafactoryname")
val subscriptionId = dbutils.widgets.get("subId")
val tenantId = dbutils.widgets.get("tenantId")
val clientId = dbutils.widgets.get("clientId")
//val clientSecret = dbutils.widgets.get("clientSecret")
val clientSecret = dbutils.secrets.get(dbutils.widgets.get("secretvault"),dbutils.widgets.get("adls_key"))
val pipelineRunId = dbutils.widgets.get("pipelinerunid")

var url = "https://login.microsoftonline.com/"+tenantId+"/oauth2/token"
var post = new HttpPost(url)
var client = HttpClientBuilder.create().build()
val nameValuePairs = new ArrayList[NameValuePair](1)
nameValuePairs.add(new BasicNameValuePair("grant_type", "client_credentials"))
nameValuePairs.add(new BasicNameValuePair("client_id", clientId))
nameValuePairs.add(new BasicNameValuePair("client_secret", clientSecret))
nameValuePairs.add(new BasicNameValuePair("resource", "https://management.azure.com"))
post.setEntity(new UrlEncodedFormEntity(nameValuePairs))
var response = client.execute(post)
var json = EntityUtils.toString(response.getEntity());
var m = jsonStrToMap(json)
println("access_token"+m)
val token = m("access_token")

url = "https://management.azure.com/subscriptions/"+subscriptionId+"/resourceGroups/"+resourceGroupName+"/providers/Microsoft.DataFactory/factories/"+dataFactoryName+"/pipelineruns/"+pipelineRunId+"/queryActivityruns?api-version=2018-06-01"
post = new HttpPost(url)
post.addHeader("Authorization", "Bearer " + token);
client = HttpClientBuilder.create().build()
response = client.execute(post)

json = EntityUtils.toString(response.getEntity());
val x = json.split(",")
x.foreach(l => {
  if(l.contains("activityRunId")){
    activityRunId = l.replaceAll("""\"""", "").replaceAll("activityRunId","").replaceAll(":", "")
  }
  if(l.contains("activityRunStart")){
    activityRunStart = l.replaceAll("""\"""", "").replaceAll("activityRunStart","").stripPrefix(":")
    println("runid is "+activityRunStart)
  }
})

if (verificationType.toLowerCase() == "landing" || verificationType.toLowerCase() == "delimit"){
  //val readLogPath = GetLogPath(verificationType, logPath)
  val yearmonth = partitionDt.take(6)
  val day = partitionDt.takeRight(2)
  val readLogPath = GetLogPath(verificationType, logPath, yearmonth, day)
   
  try
  {
    ReadCounts(readLogPath)
  } 
  catch
  {
    case _: Throwable => {
      throw new IllegalArgumentException("Failed to read log file. logPath=" + readLogPath);
      WriteToDb("Failed",activityRunId,activityRunStart)
    }
  }
  
  if(VerifyLanding()){
    WriteToDb("Succeeded",activityRunId,activityRunStart)
  }else{
    WriteToDb("Failed",activityRunId,activityRunStart)
    throw new IllegalArgumentException("Activity failed, verification failed.");
  }
}else if (verificationType.toLowerCase() == "copy"){
  if(VerifyCopy()){
    WriteToDb("Succeeded",activityRunId,activityRunStart)
  }else{
    WriteToDb("Failed",activityRunId,activityRunStart)
    throw new IllegalArgumentException("Activity failed, verification failed.");
  }
}else if (verificationType.toLowerCase() == "staging" || verificationType.toLowerCase() == "active" || (verificationType.toLowerCase() == "archive" && loadType == "full") ){
  //val readLogPath = GetLogPath(verificationType, logPath)
  val yearmonth = partitionDt.take(6)
  val day = partitionDt.takeRight(2)
  val readLogPath = GetLogPath(verificationType, logPath, yearmonth, day)
   
  try
  {
    ReadViews(readLogPath)
  } 
  catch
  {
    case _: Throwable => {
      throw new IllegalArgumentException("Failed to read log file. logPath=" + readLogPath);
      WriteToDb("Failed",activityRunId,activityRunStart)
    }
  }
  if(VerifyFullView()){
    WriteToDb("Succeeded",activityRunId,activityRunStart)
  }else{
    WriteToDb("Failed",activityRunId,activityRunStart)
    throw new IllegalArgumentException("Activity failed, verification failed.");
  }
}else if (verificationType.toLowerCase() == "archive" &&  (loadType == "cdc" || loadType == "delta")){
  //val readLogPath = GetLogPath(verificationType, logPath)
  val yearmonth = partitionDt.take(6)
  val day = partitionDt.takeRight(2)
  val readLogPath = GetLogPath(verificationType, logPath, yearmonth, day)
   
  try
  {
    ReadCDCArchiveView(readLogPath)
  } 
  catch
  {
    case _: Throwable => {
      throw new IllegalArgumentException("Failed to read log file. logPath=" + readLogPath);
      WriteToDb("Failed",activityRunId,activityRunStart)
    }
  }
  if(VerifyCDCArchive()){
    WriteToDb("Succeeded",activityRunId,activityRunStart)
  }else{
    WriteToDb("Failed",activityRunId,activityRunStart)
    throw new IllegalArgumentException("Activity failed, verification failed.");
  }
}
else{
  throw new IllegalArgumentException("Verification("+verificationType+") and Load("+loadType+") type combination is not supported.");
}
