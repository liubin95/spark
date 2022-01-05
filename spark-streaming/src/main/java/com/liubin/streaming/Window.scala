package com.liubin.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Window {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Window")
    val ssc = new StreamingContext(conf, Seconds(3))
    val lineDStream = ssc.socketTextStream("172.24.33.19", 9999)
    val word = lineDStream.map((_, 1))
    // 将多个采集周期作为整体计算
    // 滑窗函数
    // 默认是有重复数据的，每次滑动步长为1个周期
    // 设置滑动的具体步长，避免重复数据(步长大于等于窗口大小时，没有重复数据)
    val windowDS = word.window(Seconds(6), Seconds(6))
    // 关于Window的算子还有很多
    //    word.countByWindow()
    windowDS.reduceByKey(_ + _).print()
    // 启动采集器
    ssc.start()
    // 等待采集器关闭
    ssc.awaitTermination()

  }
}
