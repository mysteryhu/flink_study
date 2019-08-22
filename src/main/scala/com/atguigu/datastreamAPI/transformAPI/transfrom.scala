package com.atguigu.datastreamAPI.transformAPI

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
//TODO 转换算子
object transfrom {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val path = "D:\\Workspace\\flink_study\\src\\main\\resources\\wordcount.txt"
    val textDS = environment.readTextFile(path)

    val flatMapDS: DataStream[String] = textDS.flatMap(_.split(" "))
    //函数签名:map[R: TypeInformation](mapper: MapFunction[T, R]): DataStream[R]
    val mapDS: DataStream[(String, Int)] = flatMapDS.map((_,1))
    val filterDS = mapDS.filter(!_._1.isEmpty)
    val keyedDS: KeyedStream[(String, Int), Tuple] = filterDS.keyBy(0)
//    val value: KeyedStream[(String, Int), String] = filterDS.keyBy(_._1)
//    val keyedDS: DataStream[(String, Int)] = value


    //键控流 : 分区不分流 hash值分区
    //keyBy
    //TODO 滚动聚合算子:要聚合,先分区
    //这些算子可以针对KeyedStream的每一个支流做聚合。
//    	sum()
//    	min()
//    	max()
//    	minBy()
//    	maxBy()

    val resDS: DataStream[(String, Int)] = keyedDS.sum(1)

    //keyedDS.reduce()



    //TODO 多流转换算子
    // 1. split 和 select
    val v1 = environment.fromCollection(List(1,2,3,4,5,6,67,8,8,8,8,8,8,8))
    val v2 = environment.fromCollection(List(1,2,3,4,5,6,67,8,8,8,8,8,8,8))

    val sp1: SplitStream[Int] = v1.split { data =>
      if (data > 2) {
        Seq(">2")
      }
      else {
        Seq("<=2")
      }
    }
    val dayu2: DataStream[Int] = sp1.select(">2")
    //dayu2.print()


    //2. Connect和 CoMap
    //两个流的格式可以不同
    val v3 = environment.fromCollection(List("小张","李四","王五","赵六"))
    val v4 = environment.fromCollection(List(1,2,3,4,5,6,67,8,8,8,8,8,8,8))

    val conStream: ConnectedStreams[String, Int] = v3.connect(v4)

    val coMap: DataStream[(Any, String)] = conStream.map(
      v3Stream => (v3Stream, "zhongwen"),
      v4Stream => (v4Stream, "shuzi")
    )
    coMap.print()


    //3.union:对两个或者两个以上的DataStream进行union操作，
    // 产生一个包含所有DataStream元素的新DataStream。
    //格式要相同
    val v5 = environment.fromCollection(List(1,2,3,4,5,6,67,8,8,8,8,8,8,8))
    val v6 = environment.fromCollection(List(1,2,3,4,5,6,67,8,8,8,8,8,8,8))
    val unionDS: DataStream[Int] = v5.union(v6)
    unionDS.print()
    environment.execute()


  }
}
