package com.atguigu.datastreamAPI.sinkAPI

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.streaming.api.scala._
object RedisSink {
//TODO flink写到redis中
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()
    val path = "D:\\Workspace\\flink_study\\src\\main\\resources\\wordcount.txt"
    val textDS = environment.readTextFile(path)
    val flatMapDS: DataStream[String] = textDS.flatMap(_.split(" "))
    val resDS: DataStream[(String, Int)] = flatMapDS.filter(!_.isEmpty).map((_,1)).keyBy(_._1).sum(1)

    //保存到 Redis 中
    resDS.addSink(new RedisSink[(String,Int)](conf,new MyRedisMapper()))

    environment.execute()
  }
}

class MyRedisMapper extends RedisMapper[(String,Int)]{

  //保存到redis的命令的描述,HSET  key field value
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"wordcount")
  }

  override def getKeyFromData(t: (String, Int)): String = {
    //单词个数
    t._2.toString
  }

  override def getValueFromData(t: (String, Int)): String = {
    //单词
    t._1
  }
}