package com.atguigu.datastreamAPI.sourceAPI

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

//TODO 从文件中读取
object ReadFromText {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val textDataStream : DataStream[String] = env.readTextFile("path")
  }
}
