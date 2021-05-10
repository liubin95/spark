package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Filter {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Filter")
    val sc = new SparkContext(config)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    rdd.filter((i: Int) => i % 2 != 0).foreach(println)
  }

}
