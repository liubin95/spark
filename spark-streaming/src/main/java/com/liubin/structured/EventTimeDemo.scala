package com.liubin.structured

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * 事件时间计算
 * - 数据迟到了，设置【withWatermark】
 */
object EventTimeDemo {

  def main(args: Array[String]): Unit = {
    // 创建环境
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    // 读取数据
    // 读取数据
    val lines = spark.readStream
      .format("socket")
      .option("host", "172.21.87.21")
      .option("port", 9999)
      .load()
    //处理数据
    import spark.implicits._
    val res = lines.as[String].map((str: String) => {
      val strings = str.split(",")
      (Timestamp.valueOf(strings(0)), strings(1))
    }).toDF("timestamp", "word")
      // 水位线，数据迟到的等待时间
      .withWatermark("timestamp", "10 seconds")
      .groupBy(
        // 每隔5秒，计算最近10秒的数据
        window($"timestamp", "10 seconds", "5 seconds"), $"word"
      ).count()
    // 输出结果
    res.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()
    // 关闭资源
    spark.close()
  }

}
