package com.liubin.streaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Example2 {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]): Unit = {
    // spark 相关
    val conf = new SparkConf().setMaster("local[3]").setAppName("Example2")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("cp")

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

    // 数据DS
    val dataDs = kafkaData.map((value: ConsumerRecord[String, String]) => {
      val str = value.value()
      val strings = str.split(" ")
      AdClickCase(strings(0), strings(1), strings(2), strings(3), strings(4))
    })
    // 格式转换
    val mapDs = dataDs.map(data => {
      val st = data.st.toLong
      val date = new Date(st)
      val dateStr = dateFormat.format(date)
      ((dateStr, data.area, data.city, data.ad), 1)
    })
    // 聚合本次统计
    val reduceDs = mapDs.reduceByKey(_ + _)
    val updateStateByKey: DStream[((String, String, String, String), Int)] = reduceDs.updateStateByKey((ints: Seq[Int], option: Option[Int]) => {
      val i = option.getOrElse(0)
      // 更新到缓存区
      Option(i + ints.sum)
    })
    updateStateByKey.print()

    ssc.start()
    ssc.awaitTermination()

  }

  case class AdClickCase(st: String, area: String, city: String, user: String, ad: String)

}
