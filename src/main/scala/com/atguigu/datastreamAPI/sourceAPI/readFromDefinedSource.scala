package com.atguigu.datastreamAPI.sourceAPI

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import scala.collection.immutable
import scala.util.Random
import org.apache.flink.streaming.api.scala._
//TODO 从自定义Source 读取
object readFromDefinedSource {

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceDStream: DataStream[SensorReading] = environment.addSource(new MySensorSource())

    sourceDStream.print()
    environment.execute()

  }
}



class MySensorSource extends SourceFunction[SensorReading]{

  //flag: 表示数据是否在进行生产
  var running:Boolean=true

  /**
    * 生产数据
    * @param sourceContext
    */
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始化一个随机发生器
    val random = new Random()
    //初始化定义一组传感器温度数据
    val sensorTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(id =>
      ("sensor_" + id, 65 + random.nextGaussian() * 10D)
    )
    //无线循环,产生数据流
    while(running){

      //更新温度值
      sensorTemp.map{t=>
        (t._1,t._2+random.nextGaussian())
      }

      //获取当前时间
      val curTime = System.currentTimeMillis()

      sensorTemp.foreach(t=>
        sourceContext.collect(SensorReading(t._1,curTime,t._2))
      )
      Thread.sleep(1000)
    }

  }

  /**
    * 关闭数据源
    */
  override def cancel(): Unit = {
    false
  }
}
