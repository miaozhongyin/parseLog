package cn.leadeon.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class IntCount extends UserDefinedAggregateFunction {

  //define input field type
  override def inputSchema:StructType = {
    StructType(StructField("field1",DataTypes.StringType,true)::Nil)
  }
  //define buffer field type
  override def bufferSchema: StructType = {
     StructType(StructField("field1",DataTypes.IntegerType,true)
       ::StructField("field2",DataTypes.IntegerType,true) :: Nil)
  }

  /**
    * define result type,result will be return
    * @return
    */
  override def dataType: DataType = DataTypes.IntegerType

  override def deterministic: Boolean = false

  /**
    *
    * @param buffer
    */

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,0)
    buffer.update(1,0)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0,buffer.getInt(0)+1)
    buffer.update(1,buffer.getInt(1)+Integer.valueOf(input.getString(0)))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0,buffer1.getInt(0)+buffer2.getInt(0))
    buffer1.update(1,buffer1.getInt(1)+buffer2.getInt(1))
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getInt(1)
  }
}
