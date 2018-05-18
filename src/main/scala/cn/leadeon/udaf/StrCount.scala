package cn.leadeon.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}



class StrCount extends UserDefinedAggregateFunction {

  //define input field type
  override def inputSchema:StructType = {
    StructType(StructField("field1",DataTypes.StringType,true)::Nil)
  }
  //define buffer field type
  override def bufferSchema: StructType = {
    StructType(StructField("field1",DataTypes.StringType,true)
      ::StructField("field2",DataTypes.StringType,true) :: Nil)
  }

  /**
    * define result type,result will be return
    * @return
    */
  override def dataType: DataType = DataTypes.createMapType(DataTypes.StringType,DataTypes.IntegerType)

  override def deterministic: Boolean = false

  /**
    *
    * @param buffer
    */

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,"")
    buffer.update(1,"")
  }
  // use input data to update buffer
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0,buffer.getString(0))
    buffer.update(1,buffer.getString(1)+","+input.getString(0))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0,buffer1.getString(0)+buffer2.getString(0))
    buffer1.update(1,buffer1.getString(1)+buffer2.getString(1))
  }

  /**
    * return the end resutl
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
        val lines = buffer.getString(1)

        //import org.json4s._
        //import org.json4s.native.Serialization._
        //import org.json4s.native.Serialization
        //implicit val formats = Serialization.formats(NoTypeHints)
        //import org.json4s.DefaultFormats
        //import scala.util.parsing.json._
        import scala.collection.mutable.HashMap
        val map = HashMap[String,Int]()
        val splits = lines.split(",")
        splits.foreach(str  =>
          if (str != "" && map.get(str).getOrElse(0) == 0){
             map.put(str,1)
          }else if (str !=""){
            val value = map.get(str).get +1
            map.put(str,value)
          }
        )
//        val strBf = new ListBuffer[String]()
//
//        for((key,value) <- map){
//            strBf.append(key+":"+value)
//
//        }
//        strBf.mkString(",")
        map
  }
}
