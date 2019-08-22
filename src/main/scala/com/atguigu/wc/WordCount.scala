package com.atguigu.wc

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

object WordCount {


  def main(args: Array[String]): Unit = {

    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val path = "D:\\Workspace\\flink_study\\src\\main\\resources\\wordcount.txt"

    val textDataSet: DataSet[String] = environment.readTextFile(path)
    import org.apache.flink.api.scala._
    val resDataSet: AggregateDataSet[(String, Int)] = textDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    resDataSet.print()
  }

}
