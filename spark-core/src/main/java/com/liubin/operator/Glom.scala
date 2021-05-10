package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Glom {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Glom")
    val sc = new SparkContext(config)
    // 默认和cpu相同的分区
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // glom 和map相反的操作。将一个分区的元素，作为一个数组返回
    val value: RDD[Array[Int]] = rdd.glom()
    value.foreach((ints: Array[Int]) => println(ints.mkString("Array(", ", ", ")")))

    // 分区最大值的和
    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //    val max = rdd2.mapPartitions((ints: Iterator[Int]) => List(ints.max).iterator).sum()
    // glom 和mapPartitions 效果类似。
    val max = rdd2.glom().map((ints: Array[Int]) => ints.max).sum()
    println(max)

  }

}
