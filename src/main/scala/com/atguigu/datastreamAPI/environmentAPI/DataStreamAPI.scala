package com.atguigu.datastreamAPI.environmentAPI

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object DataStreamAPI {

  def main(args: Array[String]): Unit = {

    //TODO Environment
    //自动查询当前运行方式,调用下面两种方法获得执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //创建本地执行环境
    StreamExecutionEnvironment.createLocalEnvironment(1)
    //创建集群执行环境
    StreamExecutionEnvironment.createRemoteEnvironment("jobManager-hostname",6123,"yourPath//wordCount.jar")

    //TODO Source  数据源
  }
}
