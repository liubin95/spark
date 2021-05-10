package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Coalesce {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Coalesce")
    val sc = new SparkContext(config)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 5, 7, 8, 8789, 5))
    // 减少分区数量，减少任务调度成本
    // numPartitions 合并后分区数量
    // shuffle 是否重新洗牌
    rdd.coalesce(2, shuffle = true).saveAsTextFile("output/Coalesce")
    sc.stop()
  }

}
