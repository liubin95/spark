package com.liubin.structured.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}

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
      .option("kafka.bootstrap.servers", "172.26.182.89" + ":9092")
      .option("subscribe", "dev-log")
      .load()
    //处理数据
    val kafka: DataFrame = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    // 输出模式
    kafka.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.26.182.89" + ":9092")
      .option("topic", "dev-log-new")
      .option("checkpointLocation", "output/ckp" + System.currentTimeMillis())
      .start()
      .awaitTermination()

    spark.close()
  }

}
