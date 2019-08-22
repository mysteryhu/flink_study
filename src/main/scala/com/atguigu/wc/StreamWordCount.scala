package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val param: ParameterTool = ParameterTool.fromArgs(args)

    val host: String = param.get("host")
    val port: Int = param.getInt("port")
    val path = param.get("path")

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val textDataStream: DataStream[String] = environment.socketTextStream(host,port)

    val resultDataStream: DataStream[(String, Int)] = textDataStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //resultDataStream.print()
    resultDataStream.writeAsText(path).setParallelism(1)
    //启动 executor ,执行任务
    environment.execute("Stream Word Count")
  }
}
