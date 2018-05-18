package cn.leadeon.spark.jsonParse

import java.util
import java.util.Date

import com.alibaba.fastjson._
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * ParseLog2 use Append SaveMode to write orc file into hive table,so we need input the
  * abs inPutPath,
  */
object ParseLog2 {
  def main(args: Array[String]) {
    if (args.length !=5 ) {
      System.err.println("""Usage: inputDir :<>,outDir:<>,par_date:<>,tableName:<>,fields<>""")
      System.exit(1)
    }
    val Array(inputDir,outputDir, date,tableName,fields) = args
    val intPutPath = inputDir
    val outPutPath = outputDir + tableName.toLowerCase+"/dt="+date
    val sparkConf = new SparkConf().setAppName("parseLog2")
      //.setMaster("local[4]")
      .set("spark.sql.codegen", "false")
      .set("spark.sql.inMemoryColumnarStorage.compressed", "false")
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val sql = "select "+fields + " from " + tableName
    val hdfsRDD = sc.textFile(intPutPath).cache()
    val jsonRDD   = hdfsRDD.map(_.split("\\|\\#\\$"))
      .filter(splits => splits.size==3)
      .map(splits => {
      val serialNumber = splits(0)
      val bizCode = splits(1)
      val jsonStr = splits(2)
      val jsonObj = JSON.parseObject(jsonStr)
      // add new fields
      jsonObj.put("serialNumber", serialNumber)
      jsonObj.put("bizCode", bizCode)
      val logClean3 = new ParseLog2()
      val recursJson  = logClean3.recursionJSON(jsonObj)
      val map = new util.HashMap[String,Object]()
      logClean3.parseJSON(recursJson.asInstanceOf[JSONObject],map)
      import collection.JavaConversions._
      for{ jsonObj :JSONObject <- logClean3.resultList} yield {
        val returnJson = JSON.toJSONString(jsonObj,SerializerFeature.WriteMapNullValue)
        returnJson
      }
    }).filter(_.size>0).flatMap(list => list)
    val jsonDF = hiveContext.read.json(jsonRDD)
    val explodeDF = checkSchema(hiveContext,jsonDF)
    explodeDF.registerTempTable(tableName)
    val outPutDF = hiveContext.sql(sql)
    outPutDF.repartition(1).write.mode(SaveMode.Append).orc(outPutPath)
//    hdfsRDD.map(_.split("\\|\\#\\$"))
//      .filter(splits => splits.size !=3)
//      .map(splits => splits.mkString("|#$")).saveAsTextFile(errRecodePath)

    sc.stop()
  }



  def flattenSchema (schema :StructType,prefix :String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix+"."+f.name)
      val tarName = if (colName.contains(".")) colName.replace(".","_") else colName
      f.dataType match {
        case st:StructType => flattenSchema(st,colName)
        case _ => Array(new Column(colName).alias(tarName))
        //case _ => Array(new Column(colName))
      }
    })
  }

  def flattenSchema1(df :DataFrame,prefix :String = null) : Array[Column] = {
    df.schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix+"."+f.name)
      val tarName = if (colName.contains(".")) colName.replace(".","_") else colName
      f.dataType match {
        case st:StructType => flattenSchema(st,colName)
        case _ => Array(new Column(colName).alias(tarName))
      }
    })
  }

  /**
    * if DateFrame contain Struct type fields ,we will use lateral view outer function to explode struct fileds to mutilable rows
    * @param sQLContext
    * @param jsonDF
    * @param tabName
    * @return  explode DataFrame
    */
  def explodeSchema1(sQLContext: SQLContext,jsonDF :DataFrame,tabName:String) :DataFrame = {
    val dataTypes = jsonDF.dtypes
    val explodeSQL =  new ListBuffer[String]()
    val arrayFields = new ListBuffer[String]()
    for((name,types) <- dataTypes){
      if (types.startsWith("ArrayType")){
        arrayFields.append(" lateral view outer explode("+name+")"+" explode_"+name+" as "+name )
        explodeSQL.append(" explode_"+name+"."+name)
      }else{
        explodeSQL.append(name)
      }
    }
    val explodeSql = explodeSQL.mkString(" select ",","," from "+tabName + arrayFields.mkString)
    sQLContext.sql(explodeSql)
  }

  /**
    *
    * @param sQLContext
    * @param orgDF
    * @param nameCount
    * @return
    */
  def checkSchema(sQLContext: SQLContext,orgDF :DataFrame ,nameCount :Int = 0):DataFrame ={
    import scala.collection.mutable.HashMap
    val flagMap = HashMap[String,Int]("struct" -> 0,"array" -> 0)
    orgDF.dtypes.map(_._2).foreach(s =>{
      if (s.startsWith("ArrayType")) flagMap.put("array",flagMap.get("array").get+1)
      else if (s.startsWith("StructType")) flagMap.put("struct",flagMap.get("array").get+1)
    })
    val explodeDf = if(flagMap.get("array").get > 0){
      val tabName = "table"+ (new Date()).getTime
      orgDF.registerTempTable(tabName)
      checkSchema(sQLContext,explodeSchema1(sQLContext,orgDF,tabName))
    }else if (flagMap.get("struct").get >  0){
      val tabName = "table"+(new Date()).getTime
      orgDF.registerTempTable(tabName)
      checkSchema(sQLContext,orgDF.select(flattenSchema1(orgDF):_*))
    }else{
      orgDF
    }
    explodeDf
  }

}

class ParseLog2{
  /**
    * return result jsonObject
    */
  val resultList = new util.ArrayList[JSONObject]()

  /**
    * append father node name for child node by recurse JSON object
    * @param jsonObject  source json object
    * @param prefix      father node name ,root node prefix is ""
    * @return            target json boject
    */
  def recursionJSON(jsonObject: Any,prefix :String =""): Any = {

    jsonObject match {
      case jsonObj: JSONObject => {
        val map = new JSONObject()
        val ite = jsonObj.keySet().iterator()
        while (ite.hasNext) {
          val key = ite.next()
          val value = jsonObj.getOrDefault(key, "")
          val jsonAny = recursionJSON(value,key)
          if (prefix == "") {
            map.put(key,jsonAny)
          }
          else {
            map.put(prefix+"_"+key, jsonAny)
          }
        }
        map
      }
      case jsonArray: JSONArray => {
        val array: JSONArray = new JSONArray()
        for (index <- 0 until jsonArray.size()) {
          val jsonAny = recursionJSON(jsonArray.get(index),prefix)
          array.add(jsonAny)
        }
        array
      }
      case other: Any => other
    }
  }

  /**
    * explode json into rows
    * @param jSONObject  source json Object
    * @param hashMap     map is used to store row
    * @return            explode rows stored use list
    */
  def parseJSON(jSONObject: JSONObject,hashMap: util.HashMap[String,Object]):List[String] = {
    val keySet = jSONObject.keySet()
    import collection.JavaConversions._
    val currKeys  = keySet.toList
    var flag = false;
    for(key:String <- keySet){
      val value = jSONObject.get(key)
      if (value == null){
      }
      if (value.isInstanceOf[JSONArray]){
        flag = true
      }else{
        hashMap.put(key,value)
      }
    }
    for(key :String <- keySet){
      val value = jSONObject.get(key)
      if(value.isInstanceOf[JSONArray]){
        for(obj :Any <- value.asInstanceOf[JSONArray]){
          hashMap.keySet.removeAll(parseJSON(obj.asInstanceOf[JSONObject],hashMap))
        }
      }
    }
    if (!flag){
      //import collection.JavaConversions._
      val string :String  = JSON.toJSONString(hashMap,SerializerFeature.WriteMapNullValue)
      resultList.add(JSON.parseObject(string))
    }

    currKeys
  }

}
