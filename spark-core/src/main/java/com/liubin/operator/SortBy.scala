package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortBy {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("SortBy")
    val sc = new SparkContext(config)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 5, 7, 8, 12, 5), 2)
    // ascending true（默认）升序；false 降序
    rdd.sortBy((i: Int) => i, ascending = false).collect().foreach(println)
    sc.stop()
  }

}
