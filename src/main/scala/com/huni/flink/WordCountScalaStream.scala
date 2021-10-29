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
    //1.准备环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.socketTextStream();
  }


}
