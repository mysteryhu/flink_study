package com.atguigu.datastreamAPI.sourceAPI

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
//TODO 从自定义的集合中读取数据
object Sensor {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val sensorReadings = List(SensorReading("1", 12345, 11.1),
      SensorReading("2", 123456, 22.1),
      SensorReading("3", 123457, 33.1))

    val stream1: DataStream[SensorReading] = environment.fromCollection(sensorReadings)

    stream1.print("stream").setParallelism(3)

    environment.execute()
  }

}

//传感器样例类 传感器id,时间戳,温度
case class SensorReading(id:String,timestamp:Long,temperature:Double){}
