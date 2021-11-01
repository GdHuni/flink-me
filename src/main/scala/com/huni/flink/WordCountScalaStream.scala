package com.huni.flink

import org.apache.flink.streaming.api.scala._

/**
 * @Classname WordCountScalaStream
 * @Description 流处理统计单词 scala版本
 * @Date 2021/10/29 19:16
 * @Created by huni
 */
object WordCountScalaStream {
  def main(args: Array[String]): Unit = {
    //处理流式数据
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val streamData: DataStream[String] = environment.socketTextStream("linux121", 7777)
    //setParallelism  每个算子可以设置自己独立的并行度
    val out = streamData.flatMap(_.split(" ")).map((_, 1)).setParallelism(2).keyBy(0).sum(1)
    out.print()
    environment.execute()
  }


}
