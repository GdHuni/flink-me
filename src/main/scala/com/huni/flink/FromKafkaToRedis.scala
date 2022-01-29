package com.huni.flink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * @Classname FromKafkaToRedis
 * @Description TODO
 * @Date 2021/11/10 11:04
 * @Created by huni
 */
object FromKafkaToRedis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2.读取数据源数据
    //设置kafka配置
    val topic: String = "huni_topic"
    val props: Properties = new Properties
    props.setProperty("bootstrap.servers", "172.16.21.200:9093,172.16.5.29:9093,172.16.5.30:9093")
    props.setProperty("group.id", "mygp")

    val consumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, props)
    //设置从最新的offset开始消费
    consumer.setStartFromGroupOffsets
    consumer.setCommitOffsetsOnCheckpoints(true)
    //自动提交offset
    consumer.setCommitOffsetsOnCheckpoints(true)
    val data: DataStream[String] = env.addSource(consumer)
    val value: DataStream[(String, Int)] = data.map((_, 1)).keyBy(_._1).sum(1)
    value.print()
    env.execute()
  }

}
