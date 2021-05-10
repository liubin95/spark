package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Sample {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Sample")
    val sc = new SparkContext(config)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 7))
    // 抽取数据
    // 概率问题
    // withReplacement，抽取后继续放回数据集（true 会重复数据）
    // fraction: 每一条可能被抽取的概率
    // seed: 随机数种子(种子相同，结果相同)
    rdd.sample(withReplacement = true, 0.5).foreach(println)
    sc.stop()
  }

}
