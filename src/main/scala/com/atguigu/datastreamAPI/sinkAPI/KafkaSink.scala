package com.atguigu.datastreamAPI.sinkAPI

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.flink.streaming.api.scala._


//TODO 写到Kafka
object KafkaSink {

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(2)
    val path = "D:\\Workspace\\flink_study\\src\\main\\resources\\wordcount.txt"
    val textDS = environment.readTextFile(path)
    val flatMapDS: DataStream[String] = textDS.flatMap(_.split(" "))
    val sumDS: DataStream[(String, Int)] = flatMapDS.filter(!_.isEmpty).map((_, 1)).keyBy(_._1).sum(1)
    //放入kafka之前,必须要转成字符串格式
    val jsonDS: DataStream[String] = sumDS.map(data=>data.toString())
    jsonDS.print()

    //建立flink与kafka的连接
    val properties = new Properties()

    val flink_kafkaProducer: FlinkKafkaProducer011[String] = new FlinkKafkaProducer011[String]("hadoop102:9092","wordcount", new SimpleStringSchema())
    jsonDS.addSink(flink_kafkaProducer)

    environment.execute()




  }
}
