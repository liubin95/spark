package com.liubin

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("StateStreaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 有状态聚合，必须设定检查点目录
    ssc.checkpoint("cp")
    // 无状态的操作算子，只对当前周期内的数据处理
    val lineDStream = ssc.socketTextStream("172.24.33.19", 9999)
    val reduceDStream: DStream[(String, Int)] = lineDStream.flatMap((str: String) => str.split(" ")).map((_, 1))
    // 有状态聚合
    // 根据Key将数据和缓存的数据聚合
    // 第一个参数：新聚合的数据集合
    // 第二个参数：缓存的数据
    val stateDStream: DStream[(String, Int)] = reduceDStream.updateStateByKey((ints: Seq[Int], option: Option[Int]) => {
      val i = option.getOrElse(0)
      // 更新到缓存区
      Option(i + ints.sum)
    })
    stateDStream.print()
    // 启动采集器
    ssc.start()
    // 等待采集器关闭
    ssc.awaitTermination()

  }
}
