package com.atguigu.datastreamAPI.sourceAPI
//TODO 从kafka中消费数据
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig
object ReadFromKafka {

  def main(args: Array[String]): Unit = {
    val properties = new Properties()

    //topic sensor
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"consumer-group")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")

    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val streamConsumer: DataStream[String] = environment.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))

    streamConsumer.print().setParallelism(1)

    environment.execute()
  }
}
