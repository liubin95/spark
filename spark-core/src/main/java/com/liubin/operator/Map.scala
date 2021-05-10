package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Map {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(config)
    // map
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    rdd.map((i: Int) => {
      println(">>>>>>>>>" + i)
      i * 2
    }).foreach(println)
    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // mapPartitions 获取分区内所有元素的集合
    // 内存消耗会大一些
    rdd2.mapPartitions((ints: Iterator[Int]) => {
      println("#####Iterator")
      // 获取分区内最大值
      // 需要返回一个迭代器iterator
      List(ints.max).iterator
      //      ints.map((i: Int) => {
      //        println(s"#####$i")
      //        i * 2
      //      })
    }).foreach(println)

    val rdd3: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    rdd3.map((i: Int) => (i, i))
      .partitionBy(new MyPartitioner)
      //mapPartitionsWithIndex 带有分区索引的mapPartitions
      .mapPartitionsWithIndex((i: Int, tuples: Iterator[(Int, Int)]) => {
        if (i == 1) tuples.map((tuple: (Int, Int)) => tuple._1) else Nil.iterator
      }).foreach(println)
    sc.stop()
  }
}
