package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Join {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Join")
    val sc = new SparkContext(config)
    val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "C"), (4, "C")))
    val rdd2: RDD[(Int, String)] = sc.makeRDD(Array((1, "A"), (2, "B"), (3, "c")))
    // (k,v).join((k,w))=>(k,(v,w))
    // inner join ，两个都存在，才会出来
    // 如果数据中存在多个相同key，产生笛卡尔积
    rdd1.join(rdd2).foreach(println)
    // (k,v).join((k,w))=>(k,(v,Option[w]))
    // 左连接和sql相同
    rdd1.leftOuterJoin(rdd2).foreach(println)
    sc.stop()
  }

}
