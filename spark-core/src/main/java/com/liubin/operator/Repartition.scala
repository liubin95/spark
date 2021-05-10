package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Repartition {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Repartition")
    val sc = new SparkContext(config)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 5, 7, 8, 12, 5), 2)
    // 底层就是coalesce(numPartitions, shuffle = true)
    // 增加分区数量
    rdd.mapPartitions((ints: Iterator[Int]) => {
      println(ints)
      ints
    })
    rdd.repartition(4)
    rdd.mapPartitions((ints: Iterator[Int]) => {
      println(ints)
      ints
    })
    rdd.collect().foreach(println)
    sc.stop()
  }

}
