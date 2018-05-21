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
  *  ParseLog 主要实现解析log 中的JSON 数据，并将最终需要的字段写入到指定的hive 目录下。
  */
object ParseLog {
  def main(args: Array[String]) {
    if (args.length !=5 ) {
      System.err.println("""Usage: inputDir :<>,outDir:<>,par_date:<>,tableName:<>,fields<>""")
      System.exit(1)
    }
    val Array(inputDir,outputDir, date,tableName,fields) = args
    val intPutPath = inputDir + date + "/" + tableName.toUpperCase + date +"*.log"
    val outPutPath = outputDir + tableName.toLowerCase+"/dt="+date
    //val errRecodePath = outputDir + tableName.toLowerCase+"/errRecode/"+date
    val sparkConf = new SparkConf().setAppName("parseLog")
      //.setMaster("local[4]")
      .set("spark.sql.codegen", "false")
      .set("spark.sql.inMemoryColumnarStorage.compressed", "false")
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val sql = "select "+fields + " from " + tableName
    val hdfsRDD = sc.textFile(intPutPath).cache()
    // 完成jSON 数据 解析为多行记录功能
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
      val logClean3 = new ParseLog()
      val recursJson  = logClean3.recursionJSON(jsonObj)
      val map = new util.HashMap[String,Object]()
      logClean3.parseJSON(recursJson.asInstanceOf[JSONObject],map)
      // for 语句中返回的时每条JSON 数据解析后的 多条记录List，在flatMap 中依次将该list 展平为单行JSON记录
      import collection.JavaConversions._
      for{ jsonObj :JSONObject <- logClean3.resultList} yield {
        val returnJson = JSON.toJSONString(jsonObj,SerializerFeature.WriteMapNullValue)
        returnJson
      }
    }).filter(_.size>0).flatMap(list => list)
    val jsonDF = hiveContext.read.json(jsonRDD)
    // 检查jsonDF 中是否包含 StructType 数据类型。该过程可以根据具体需求来决定是否调用。
    val explodeDF = checkSchema(hiveContext,jsonDF)
    explodeDF.registerTempTable(tableName)
    // 通过传入的json字段，对DataFrame 中的数据进行筛选。
    val outPutDF = hiveContext.sql(sql)
    // 输出结果到hive 中
    outPutDF.write.mode(SaveMode.Overwrite).orc(outPutPath)
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
    * 调用 hive 中的 lateral view 语句展开 dataFrame 中的数组列为多行记录，类似spark 中的 explode 函数，但explode 函数在
    * sql 语句中只能使用一次，不能同时平级的展开多个数组列，所以选择hive 语句中的lateral view 。有关lateral view 更详细用法查看有关
    * 文档。
    * 经生产环境大量数据测试，该 函数的在处理大量数据时，需要占用较大的内存空间，且数据处理主要发生在Drive 端，给driver 端节点性能
    * 带来较大压力，建议应减少对该语句的调用，或在最后需要该功能时调用。
    * @param sQLContext
    * @param jsonDF
    * @param tabName
    * @return new DataFrame ，can't contain ArrayType column
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
    *检查 DataFrame 中是否包含Array or StructType 类型。并根据DataFrame 中 Array or StructType 类型调用不同的函数展开对应的列。
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
class ParseLog{
  val resultList = new util.ArrayList[JSONObject]()

  /**
    * 该函数通过递归方法，给json 中的所有子节点增加父节点名称，消除json 中的同名节点。
    * 可以根据具体情况来决定是否调用该方法。
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
    * 该方法为递归方法，用于解析json 数据为多行记录，实现explode 功能，
    * 需要注意，当叶子节点为DataFrame 中的StructType 时(or java Map类型)，默认为JSONObject 类型返回。
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
