package com.atguigu.datastreamAPI.sinkAPI

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
//TODO flink数据写到MySQLSINK
object jdbcsink {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val path = "D:\\Workspace\\flink_study\\src\\main\\resources\\wordcount.txt"
    val textDS = environment.readTextFile(path)
    val flatMapDS: DataStream[String] = textDS.flatMap(_.split(" "))
    val resDS: DataStream[(String, Int)] = flatMapDS.filter(!_.isEmpty).map((_, 1)).keyBy(_._1).sum(1)

    resDS.addSink(new MyJdbcSink())

    resDS.print()
    environment.execute()
  }
}


class MyJdbcSink() extends RichSinkFunction[(String, Int)] {

  var conn: Connection = _
  var insertStatement: PreparedStatement = _
  var updateStatement: PreparedStatement = _

  //invoke方法之前调用,做一些初始化操作
  override def open(parameters: Configuration): Unit = {
    //定义连接,PreparedStatement(预编译)
    conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/flink0311", "root", "abc123")
    //插入操作
    insertStatement = conn.prepareStatement("INSERT into wordcount (word,count) values (?,?)")
    //更新操作
    updateStatement = conn.prepareStatement("update wordcount set count = ? where word = ?")
  }


  //每一条数据会调用一次invoke
  override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
    //    super.invoke(value, context)
    //更新的字段 word
    updateStatement.setInt(1, value._2)
    //更新的字段 count
    updateStatement.setString(2, value._1)
    //提交更新
    updateStatement.execute()
    //判断数据库是否有该word
    if (updateStatement.getUpdateCount() == 0) {
      //没有,就做插入操作
      insertStatement.setString(1, value._1)
      insertStatement.setInt(2, value._2)
      insertStatement.execute()
    }
  }


  //关闭资源,连接等操作
  override def close(): Unit = {
    insertStatement.close()
    updateStatement.close()
    conn.close()

  }
}