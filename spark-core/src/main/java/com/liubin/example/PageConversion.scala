package com.liubin.example

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>【单跳】：一次session中，用户访问页面顺序为【3-5-7-9】。则【3-5】为一次单跳，【5-7】也是一次单跳
 * <p>【单跳转化率】：对于【3】页面的访问数【A】；然后统计从【3】页面到【5】页面的访问数B。那么【B/A】就是【3-5】的单跳转化率
 *
 *
 */
object PageConversion {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("PageConversion")
    val sc: SparkContext = new SparkContext(config)
    val file: RDD[String] = sc.textFile("input/user_visit_action.txt")
    val dataRdd: RDD[UserVisitData] = file.map((str: String) => {
      val strings: Array[String] = str.split("_")
      UserVisitData(strings(0),
        strings(1),
        strings(2),
        strings(3).toLong,
        strings(4),
        strings(5),
        strings(6).toLong,
        strings(7).toLong,
        strings(8),
        strings(9),
        strings(10),
        strings(11),
        strings(12).toLong)
    })

    dataRdd.cache()

    // 处理分母
    // (page,visitNum)
    val pageVisitMap: collection.Map[Long, Int] = dataRdd.map((data: UserVisitData) => (data.pageId, 1))
      .reduceByKey(_ + _)
      .collectAsMap()

    // 处理分子
    val groupByRdd = dataRdd.groupBy(_.sessionId)
    // 时间排序
    // (session,[pages])
    val pageSortRdd = groupByRdd.mapValues((datas: Iterable[UserVisitData]) => {
      datas.toList.sortBy((data: UserVisitData) => {
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(data.actionTime)
      }).map(_.pageId)
    })
    // [1,2,3,4]=>[1-2,2-3,3-4]
    // 滑窗
    val pageToPage: RDD[List[((Long, Long), Int)]] = pageSortRdd.map((tuple: (String, List[Long])) => {
      // 拉链zip
      val pageToPage = tuple._2.zip(tuple._2.tail)
      // ((page,toPage),1)
      // 如果需要过滤在此处加filter
      pageToPage.map((tuple: (Long, Long)) => ((tuple._1, tuple._2), 1))
    })
    // 扁平化处理
    val flatMap: RDD[((Long, Long), Int)] = pageToPage.flatMap(item => item)
    // ((page,toPage),sum)
    val reduceRes: RDD[((Long, Long), Int)] = flatMap.reduceByKey(_ + _)
    val result = reduceRes.collect()

    // 分子除以分母
    result.foreach {
      case ((page, toPage), sum) =>
        val pageVisitNum = pageVisitMap.getOrElse(page, 0)
        println(s"${page}到页面${toPage}的转换率为：${sum.toDouble / pageVisitNum}")
    }

    sc.stop()
  }
}
