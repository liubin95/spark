package com.liubin.streaming

import java.io.{FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object Example3 {

  val dateFormat = new SimpleDateFormat("mm:ss")


  def main(args: Array[String]): Unit = {
    // spark 相关
    val conf = new SparkConf().setMaster("local[3]").setAppName("Example3")
    val ssc = new StreamingContext(conf, Seconds(5))

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
    // 最近一分钟 每10s统计一次
    val mapDs = dataDs.map(data => {
      val st = data.st.toLong
      // 将时间全部设置成整数秒
      val l = st / 10000 * 10000
      (l, 1)
    })
    // 窗口计算
    // 60秒：窗口大小
    // 10秒：计算一次
    val reduceDs = mapDs.reduceByKeyAndWindow((i: Int, i1: Int) => i + i1, Seconds(60), Seconds(10))
    // 结合echarts
    reduceDs.foreachRDD(rdd => {
      val list = ListBuffer[String]()
      val array = rdd.sortByKey(ascending = false).collect()
      array.foreach((tuple: (Long, Int)) => {
        val str = dateFormat.format(new Date(tuple._1))
        list.append(s"""{ "xtime":"$str", "yval":"${tuple._2}" }""")
      })
      val out = new PrintWriter(new FileWriter("C:\\Users\\huawei\\IdeaProjects\\spark\\input\\adclick\\adclick.json"))
      println(array.mkString("Array(", ", ", ")"))
      out.println("[" + list.mkString(",") + "]")
      out.flush()
      out.close()
    })

    ssc.start()
    ssc.awaitTermination()

  }

  case class AdClickCase(st: String, area: String, city: String, user: String, ad: String)

}
