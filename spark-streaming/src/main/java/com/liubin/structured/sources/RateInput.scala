package com.liubin.structured.sources

import org.apache.spark.sql.SparkSession

object RateInput {

  def main(args: Array[String]): Unit = {
    // 创建环境
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("RateInput")
      .getOrCreate()
    // 读取数据
    val lines = spark.readStream
      .format("rate")
      .option("rowsPerSecond ", "10")
      .option("rampUpTime ", "0s")
      .option("numPartitions ", "2")
      .load()


    // 输出结果
    val query = lines.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", value = false)
      .start()
    // 启动，等待结束
    query.awaitTermination()
    // 关闭资源
    spark.close()
  }

}
