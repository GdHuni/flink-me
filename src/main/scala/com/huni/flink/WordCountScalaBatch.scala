package com.huni.flink

import org.apache.flink.api.scala._

/**
 * @Classname WordCountScalaBatch
 * @Description scala版本的flink批处理
 * @Date 2021/10/28 19:58
 * @Created by huni
 */
object WordCountScalaBatch {
  def main(args: Array[String]): Unit = {
    //定义输入输出文件
    val in: String = "D:\\A_E\\hello.txt"
    val out: String = "D:\\A_E\\outputscala.txt"
    //定义flink运行环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val value: DataSet[String] = environment.readTextFile(in);
    //实现自己的业务
    val value1: AggregateDataSet[(String, Int)] = value.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    //输出到文件
    value1.writeAsText(out).setParallelism(1)

    //执行（懒加载的）
    environment.execute()

  }

}