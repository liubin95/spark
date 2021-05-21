package com.liubin

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransForm {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TransForm")
    val ssc = new StreamingContext(conf, Seconds(3))
    val lineDStream = ssc.socketTextStream("172.24.33.19", 9999)

    // 获取底层的rdd,直接操作rdd
    // 1.DStream功能不完善
    // 2.周期性执行代码


    // code:Driver 执行
    val rdd: DStream[String] = lineDStream.transform(
      // code:Driver 执行（每个周期执行一次）
      (value: RDD[String]) => {
        value.map((str: String) => {
          // code:Executor 执行
          str
        })
      })

    // 直接操作DStream
    // code:Driver 执行
    lineDStream.map((str: String) => {
      // code:Executor 执行
      str
    })

    // 启动采集器
    ssc.start()
    // 等待采集器关闭
    ssc.awaitTermination()
  }
}
