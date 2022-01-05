package com.liubin.structured.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.liubin.mock.MockIotDatas.DeviceData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object KafkaDemoIot {

  def main(args: Array[String]): Unit = {
    // 创建环境
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    // 读取数据
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.26.182.89" + ":9092")
      .option("subscribe", "iot-topic")
      .load()
    //处理数据
    val objectMapper = new ObjectMapper()
    import spark.implicits._
    val kafka = df.selectExpr("CAST(value AS STRING)")
      .as[String]
      // 解析json
      .map((str: String) => {
        objectMapper.registerModule(DefaultScalaModule)
        objectMapper.readValue(str, classOf[DeviceData])
      })
    kafka.createOrReplaceTempView("iot_data")
    val res = spark.sql(
      """
        |select deviceType,count(*) as deviceType_num, avg(signal) as avg_signal
        |from iot_data
        |where signal > 30
        |group by deviceType
        |""".stripMargin)

    // 输出模式
    res.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()

    spark.close()
  }

}
