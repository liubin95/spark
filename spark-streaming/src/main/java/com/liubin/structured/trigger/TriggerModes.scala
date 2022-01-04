package com.liubin.structured.trigger

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object TriggerModes {

  def main(args: Array[String]): Unit = {
    // 创建环境
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("TriggerModes")
      .getOrCreate()
    // 读取数据
    val ds = spark.readStream
      .format("socket")
      .option("host", "172.25.203.166")
      .option("port", 9999)
      .load()
    //处理数据
    import spark.implicits._
    val res = ds.as[String].flatMap(_.split(" "))

    // 输出模式
    val query = res.writeStream
      .format("console")
      .outputMode("append")
      // 触发时机
      // Once 触发一次 离线处理
      // ProcessingTime 微批处理 - 默认(0 尽快运行)
      // Continuous 流式处理 ，指定检查点(检查点 时间 间隔)
      .trigger(Trigger.Continuous("1 seconds"))
      // 设置检查点
      .option("checkpointLocation", "output/ckp" + System.currentTimeMillis())
      .start()


    query.awaitTermination()
    spark.close()
  }

}
