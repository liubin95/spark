package com.liubin

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Queue {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Queue")
    // 采集周期
    val ssc = new StreamingContext(conf, Seconds(3))
    val queue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
    val inputStream: InputDStream[Int] = ssc.queueStream(queue, oneAtATime = false)
    val result: DStream[(Int, Int)] = inputStream.map((_, 1)).reduceByKey(_ + _)
    result.print()
    // 启动采集器
    ssc.start()

    for (i <- 1 to 5) {
      queue += ssc.sparkContext.makeRDD(1 to 300, numSlices = 10)
      Thread.sleep(2000)
    }
    // 等待采集器关闭
    ssc.awaitTermination()

  }

}
