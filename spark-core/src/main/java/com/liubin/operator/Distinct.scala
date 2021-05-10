package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Distinct {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Distinct")
    val sc = new SparkContext(config)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 1))
    // 底层使用reduceByKey 去重
    rdd.distinct().foreach(println)
    sc.stop()
  }

}
