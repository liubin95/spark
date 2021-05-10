package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object GroupByKey {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("GroupByKey")
    val sc = new SparkContext(config)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    // 自定义分区规则
    // 键值对类型才可以分区
    // 类型和数量一样的分区器，只会执行一次
    val mapRdd = rdd.map((i: Int) => (if (i % 2 == 0) 1 else 0, i))
    mapRdd.partitionBy(new HashPartitioner(2)).saveAsTextFile("output/partitionBy")

    // 相同的key，两两聚合
    mapRdd.reduceByKey((i: Int, j: Int) => i + j).foreach(println)

    // 相同的key，返回一个（key，List）
    mapRdd.groupByKey().foreach(println)
    // groupBy 不会将元素的value整合，返回的是元组数组
    val groupByRdd: RDD[(Int, Iterable[(Int, Int)])] = mapRdd.groupBy((tuple: (Int, Int)) => tuple._1)

    //groupByKey 和 reduceByKey的区别
    // reduceByKey 分区内先聚合，在分组聚合（效率高一些）
    // groupByKey  直接分组之后再聚合

    // 也是根据Key 聚合
    // 分区内和分区间聚合方式不同
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 3), ("a", 7), ("b", 9), ("b", 1)))
    // 第一个参数列表 初始值。 第一个元素和初始值进行运算
    // 第二个参数列表
    // 第二个参数列表-1 ：分区内操作
    // 第二个参数列表-2 ：分区间操作
    rdd2.aggregateByKey(0)(
      // 分区内最大值
      (i, j) => math max(i, j),
      // 分区间合并
      (x, y) => x + y
    ).foreach((tuple: (String, Int)) => println(tuple))
  }

}
