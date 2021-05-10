package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Intersection {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Intersection")
    val sc = new SparkContext(config)
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 3, 5, 7, 9))
    val rdd2: RDD[Int] = sc.makeRDD(List(2, 4, 6, 8, 9))
    // 交、并、差。需要类型相同
    // 交集
    rdd1.intersection(rdd2).foreach(println)
    // 并集
    rdd1.union(rdd2).foreach(println)
    // 差集
    // rdd1 - 部分
    rdd1.subtract(rdd2).foreach(println)
    //拉链
    // 分区数量和分区中元素数量 都要相同
    // 类型可以不同
    rdd1.zip(rdd2).foreach(println)

    sc.stop()
  }
}
