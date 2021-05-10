package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CoGroup {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("CoGroup")
    val sc = new SparkContext(config)
    val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (2, "C"), (4, "C")))
    val rdd2: RDD[(Int, String)] = sc.makeRDD(Array((1, "A"), (2, "B"), (3, "c")))
    // 分组 + 链接
    // 根据key链接不同的rdd，key一样的在一组
    rdd1.cogroup(rdd2).foreach(println)
    // 可以连接多个
    rdd1.cogroup(rdd2, rdd2, rdd2).foreach(println)
    sc.stop()
  }

}
