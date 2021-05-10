package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Agent {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Agent")
    val sc = new SparkContext(config)
    //时间戳 省份 城市 用户 广告
    val file: RDD[String] = sc.textFile("input/agent.log")
    //((6,16),1)
    file.map((str: String) => {
      val strings = str.split(" ")
      ((strings(1), strings(4)), 1)
    })
      //((6,16),sum)
      .reduceByKey((i: Int, i1: Int) => i + i1)
      //(6,(16,sum))
      .map {
        case ((prv, ad), sum) =>
          (prv, (ad, sum))
      }
      //(6,[(16,sum),...])
      .groupByKey()
      //(6,[(16,sum),...])
      // 对集合排序，取前三
      .mapValues((tuples: Iterable[(String, Int)]) => {
        tuples.toList.sortBy((tuple: (String, Int)) => tuple._2)(Ordering.Int.reverse).take(3)
      })
      .foreach(println)

    sc.stop()
  }

  // rdd算子中使用的类，都需要实现序列化
  class User1 extends Serializable {
    var age = 30
  }

  // 编译时，自动实现序列化
  case class User2() {
    var age = 30

  }

}

