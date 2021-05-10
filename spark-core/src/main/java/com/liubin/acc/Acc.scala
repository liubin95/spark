package com.liubin.acc

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 分布式只写变量
 * <p>
 * */
object Acc {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val sc = new SparkContext(config)

    val numRDD = sc.makeRDD(List(1, 2, 3, 4))
    val sum = numRDD.reduce(_ + _)
    println(sum)

    // 原子累加器
    // 累加器的执行和行动算子有关系
    val sumAccumulator: LongAccumulator = sc.longAccumulator("sum")
    // 集合
    //sc.collectionAccumulator()
    // 浮点数
    //sc.doubleAccumulator()
    // 累加操作
    numRDD.foreach((i: Int) => sumAccumulator.add(i))
    // 获取累加的值
    println(sumAccumulator.value)
    sc.stop()

  }

}
