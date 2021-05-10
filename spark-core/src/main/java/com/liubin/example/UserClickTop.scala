package com.liubin.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//  再热门top10基础上，每个品类用户session的点击统计
object UserClickTop {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("UserClickTop")
    val sc: SparkContext = new SparkContext(config)
    val file: RDD[String] = sc.textFile("input/user_visit_action.txt")
    val dataRdd: RDD[UserVisitData] = file.map((str: String) => {
      val strings: Array[String] = str.split("_")
      UserVisitData(strings(0),
        strings(1), strings(2), strings(3).toLong, strings(4), strings(5), strings(6).toLong,
        strings(7).toLong, strings(8), strings(9), strings(10), strings(11), strings(12).toLong)
    })
    dataRdd.cache()
    val top10Rdd: Array[Long] = hotCategoryTop10_3(dataRdd)
    // 点击过滤
    val filterRdd = dataRdd.filter(data => data.clickCategoryId != -1 && top10Rdd.contains(data.clickCategoryId))
    val mapRdd = filterRdd.map((data: UserVisitData) => ((data.clickCategoryId, data.sessionId), 1))
    // ((cate,session),sum)
    val reduceRdd = mapRdd.reduceByKey(_ + _)
    // (cate,(session,sum))
    val result = reduceRdd.map(t => (t._1._1, (t._1._2, t._2)))
    // (cate,[(session,sum)...])
    val groupRdd = result.groupByKey()
    // 每一个分类下，排序，取10
    val resRdd = groupRdd.mapValues((tuples: Iterable[(String, Int)]) => {
      val list = tuples.toList
      list.sortBy((tuple: (String, Int)) => tuple._2)(Ordering.Int.reverse).take(10)
    })
    resRdd.collect().foreach(println)
    sc.stop()
  }

  def hotCategoryTop10_3(dataRdd: RDD[UserVisitData]): Array[Long] = {
    val allDataRdd = dataRdd.flatMap((data: UserVisitData) => {
      if (data.clickCategoryId != -1) {
        // click
        Array((data.clickCategoryId, (1, 0, 0)))
      } else if (!data.orderCategoryIds.equals("null")) {
        // order
        val orderIds = data.orderCategoryIds.split(",")
        orderIds.map(str => (str.toLong, (0, 1, 0)))
      } else if (!data.payCategoryIds.equals("null")) {
        // pay
        val payIds = data.payCategoryIds.split(",")
        payIds.map(str => (str.toLong, (0, 0, 1)))
      } else {
        Nil
      }
    })

    // (类别，（点击，下单，支付）)
    val resMap = allDataRdd.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })
    //排序
    val sortRdd = resMap.sortBy((tuple: (Long, (Int, Int, Int))) => tuple._2, ascending = false)

    sortRdd.take(10).map(_._1)
  }

}
