package com.liubin.streaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object MockData {
  val areaList: mutable.Seq[String] = ListBuffer[String]("东北", "华北", "华南", "华北", "东北")
  val cityList: mutable.Seq[String] = ListBuffer[String]("大连", "北京", "深圳", "张家界", "沈阳")

  def main(args: Array[String]): Unit = {
    // kafka配置信息
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val kafkaProducer = new KafkaProducer[String, String](prop)

    while (true) {
      mockData().foreach((str: String) => {
        println(str)
        val record = new ProducerRecord[String, String]("spark", null, str)
        kafkaProducer.send(record)
      })
      Thread.sleep(2 * 1000)
    }
  }

  def mockData(): ListBuffer[String] = {
    val list = ListBuffer[String]()
    for (a <- 1 to Random.nextInt(50)) {
      // 时间戳 区域 城市 用户 广告
      val i = Random.nextInt(5)
      list.append(f"${System.currentTimeMillis()} ${areaList(i)} ${cityList(i)} ${Random.nextInt(10)} ${Random.nextInt(10)}")
    }
    list
  }
}
