package com.atguigu.datastreamAPI.transformAPI

import com.atguigu.datastreamAPI.sourceAPI.{MySensorSource, SensorReading}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ArrayBuffer
object DefinedFunction {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
//    val originDS: DataStream[(String, Int, Double)] = environment.fromElements(("sensor1",12345,43.1))
//    originDS.map{data=>
//      SensorReading(data._1,data._2,data._3)
//    }

    val sensorDS = environment.addSource(new MySensorSource)
    val reduceDS: DataStream[(String, Long, Double, Int)] = sensorDS.map(sensor => (sensor.id, sensor.timestamp, sensor.temperature, 1))
      .keyBy(0)
      .reduce((a, b) => (a._1, b._2, a._3 + b._3, a._4 + b._4))

    val redDS: DataStream[(String, Long, Double, Int)] = reduceDS.filter(new MyFilter("1"))
    redDS.print()
    environment.execute()
  }
}
//TODO 自定义UDF函数
class MyFilter(keyword:String) extends FilterFunction[(String, Long, Double, Int)]{
  override def filter(value: (String, Long, Double, Int)): Boolean = {
    value._1.contains(keyword)
  }
}
