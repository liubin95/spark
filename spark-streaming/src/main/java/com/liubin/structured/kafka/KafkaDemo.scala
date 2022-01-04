package com.liubin.structured.kafka

import org.apache.spark.sql.SparkSession

object KafkaDemo {

  def main(args: Array[String]): Unit = {
    // 创建环境
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("KafkaDemo")
      .getOrCreate()
    // 读取数据
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.25.203.166:9092")
      .option("subscribe", "dev-log")
      .load()
    //处理数据
    import spark.implicits._
    val kafka = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    // 输出模式
    kafka.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()


    spark.close()
  }

}
