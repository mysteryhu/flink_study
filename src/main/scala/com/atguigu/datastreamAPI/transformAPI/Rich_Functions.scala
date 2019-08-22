package com.atguigu.datastreamAPI.transformAPI

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

//关于富函数:可以获取运行环境的上下文,拥有声明周期方法,可以实现更复杂的功能
object Rich_Functions {

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val path = "D:\\Workspace\\flink_study\\src\\main\\resources\\wordcount.txt"
    val textDS = environment.readTextFile(path)
    val flatMapDS: DataStream[String] = textDS.flatMap(_.split(" "))
    val mapDS: DataStream[(String, Int)] = flatMapDS.map(new MyMapFunction())

    val redDS: DataStream[(String, Int)] = mapDS.keyBy(0).reduce((w1, w2)=>(w1._1,w1._2+w2._2))

    redDS.print()
    environment.execute()
  }

}


class MyMapFunction extends RichMapFunction[String,(String,Int)]{

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def map(value: String): (String, Int) = {
    (value,1)
  }

  @throws[Exception]
  override def close(): Unit = {
    super.close()
  }
}