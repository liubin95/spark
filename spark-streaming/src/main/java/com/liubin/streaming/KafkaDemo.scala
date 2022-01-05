package com.liubin.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaDemo")
    val ssc = new StreamingContext(conf, Seconds(3))

    // kafka配置信息
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "spark_group",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // 卡夫卡工具类，泛型为数据key和value
    val kafkaData: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      // 采集的节点和计算节点的匹配规则
      // 自动选择
      LocationStrategies.PreferConsistent,
      // 订阅信息
      ConsumerStrategies.Subscribe[String, String](Set("spark"), kafkaParams)
    )

    kafkaData.map(_.value()).print()


    ssc.start()
    ssc.awaitTermination()

  }

}
