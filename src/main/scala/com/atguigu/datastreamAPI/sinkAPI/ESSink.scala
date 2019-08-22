package com.atguigu.datastreamAPI.sinkAPI

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
object ESSink {
  def main(args: Array[String]): Unit = {

    //Flink环境准备
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val path = "D:\\Workspace\\flink_study\\src\\main\\resources\\wordcount.txt"
    val textDS = environment.readTextFile(path)
    val flatMapDS: DataStream[String] = textDS.flatMap(_.split(" "))
    val resDS: DataStream[(String, Int)] = flatMapDS.filter(!_.isEmpty).map((_,1)).keyBy(_._1).sum(1)


    //TODO 定义ESSink
    //ES 发送请求是http请求
    //定义一个http host
    val httpHost = new java.util.ArrayList[HttpHost]()
    httpHost.add(new HttpHost("hadoop102",9200))

    //创建一个 es sink的builder
    val esSinkBuilder: ElasticsearchSink.Builder[(String, Int)] = new ElasticsearchSink.Builder[(String, Int)](
      httpHost,
      new ElasticsearchSinkFunction[(String, Int)] {
        override def process(element: (String, Int), ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          //准备要写入的数据source
          val json = new java.util.HashMap[String, String]()
          json.put("word", element._1)
          json.put("count", element._2.toString)
          //创建 index request
          val indexRequest: IndexRequest = Requests.indexRequest().index("wordcount").`type`("_doc").source(json)
          //发出http请求
          indexer.add(indexRequest)
          println("save" + element._1 + "SUCCESS")
        }
      })

    resDS.addSink(esSinkBuilder.build())

    environment.execute()
  }
}
