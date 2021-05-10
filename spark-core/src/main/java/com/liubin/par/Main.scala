package com.liubin.par

import com.liubin.operator.MyPartitioner
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("par")
    val sc = new SparkContext(config)
    val seq = Seq[Int](1, 2, 6, 98, 46, 54, 6)
    // 可以传递分区数量,相当于reducer数量
    val rdd = sc.makeRDD(seq, 2).map((i: Int) => (i, i))
      // 自定义分区规则
      .partitionBy(new MyPartitioner)
    rdd.saveAsTextFile("output")
  }
}
