package cn.leadeon.spark.jsonParse

package cn.leadeon.spark.jsonParse

import java.util.Date
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
object CatJsonSchema {
  case class BosData(serialNumber:String,bizCode:String,bosData: String)
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("""Usage: inputDir :<>,par_date<> ,tableName:<>""")
      System.exit(1)
    }
    //  val from args :
    //  hdfs://hadoop1:8020/data/bos-json/
    //  hdfs://hadoop1:8020/data/bos_log/
    //  BOS_QSCORE_CATE_DTL
    //  serialName,bizCode,brand,point_pointName,point_pointValue,pointValue
    //  20180409
    val Array(inputDir, date,tableName) =args
    val inputPath = inputDir+tableName.map(_.toUpper)+date+".json"
    val sparkConf = new SparkConf().setAppName("JsonToORC")
      .setMaster("local[4]")
      .set("spark.sql.codegen", "false")
      .set("spark.sql.inMemoryColumnarStorage.compressed", "false")
      .set("spark.sql.inMemoryColumnarStorage.batchSize", "1000")
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    //1 read json file and register jsonTable
    val jsonDF = sqlContext.read.json(inputPath)
    println("-------------------------json_schema:------------------------")
    jsonDF.printSchema()
    //2 explode dataFrame which contain Array or map type
    val explodeDF = checkSchema(sqlContext,jsonDF)
    println("-------------------------explode_schema:------------------------")
    explodeDF.printSchema()
    explodeDF.show(10,false)
    //3 write explodeDF to hive table
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
    sQLContext.sql(explodeSql).toDF()

  }
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
