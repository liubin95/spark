package com.liubin

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Resume {
  def main(args: Array[String]): Unit = {

    // 从检查点恢复数据
    val ssc = StreamingContext.getActiveOrCreate("cp", () => {
      val conf = new SparkConf().setMaster("local[*]").setAppName("Resume")
      val ssc = new StreamingContext(conf, Seconds(3))
      ssc
    })
    ssc.start()
    ssc.awaitTermination()


  }
}
