package com.liubin.structured

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object DropDuplicatesDemo {

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
      // 根据列 去重数据
      .dropDuplicates("timestamp", "word")
    // 水位线，数据迟到的等待时间
    // 输出结果
    res.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("truncate", "false")
      .start()
      .awaitTermination()
    // 关闭资源
    spark.close()
  }
}
