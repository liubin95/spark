package com.liubin.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 四种行为数据：【搜索】，【点击】，【下单】，【支付】 每一条行为数据，只能是一种行为
 *
 * <p>【-1】和【null】都是代表无效值
 *
 * <p>【下单】、【支付】可能有多个商品，以【,】分割
 *
 * <p>2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_37_2019-07-17
 * 00:00:02_手机_-1_-1_null_null_null_null_3
 *
 * <p>日期_用户id_sessionId_页面id_动作时间_搜索关键字_点击品类id_点击产品id_下单品类id_下单产品id_支付品类id_支付产品id_城市id
 */
object UserVisit {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("UserVisit")
    val sc: SparkContext = new SparkContext(config)
    val file: RDD[String] = sc.textFile("input/user_visit_action.txt")
    val dataRdd: RDD[UserVisitData] = file.map((str: String) => {
      val strings: Array[String] = str.split("_")
      UserVisitData(strings(0),
        strings(1), strings(2), strings(3).toLong, strings(4), strings(5), strings(6).toLong,
        strings(7).toLong, strings(8), strings(9), strings(10), strings(11), strings(12).toLong)
    })
    hotCategoryTop10_3(dataRdd)
    sc.stop()
  }

  /*
  * 品类倒序：点击数量，下单数量，支付数量
  * */
  def hotCategoryTop10(dataRdd: RDD[UserVisitData]): Unit = {
    //(类别，（点击，下单，支付）)

    //点击
    val clickFilter = dataRdd.filter((data: UserVisitData) => data.clickCategoryId != -1)
    val clickGroupBy: RDD[(Long, Iterable[UserVisitData])] = clickFilter.groupBy((data: UserVisitData) => data.clickCategoryId)
    //(类别，点击数)
    val clickNum: RDD[(Long, Int)] = clickGroupBy.map((tuple: (Long, Iterable[UserVisitData])) => (tuple._1, tuple._2.size))

    // 下单
    val orderFilter = dataRdd.filter((data: UserVisitData) => !data.orderCategoryIds.equals("null"))
    val orderFlatMap = orderFilter.flatMap((data: UserVisitData) => {
      val orderIds = data.orderCategoryIds.split(",")
      // （类别，1）
      orderIds.map((str: String) => (str.toLong, 1))
    })
    //(类别，下单数)
    val orderNum: RDD[(Long, Int)] = orderFlatMap.reduceByKey(_ + _)

    //支付
    val payFilter = dataRdd.filter((data: UserVisitData) => !data.payCategoryIds.equals("null"))
    val payFlatMap = payFilter.flatMap((data: UserVisitData) => {
      val payIds = data.payCategoryIds.split(",")
      // （类别，1）
      payIds.map((str: String) => (str.toLong, 1))
    })
    //(类别，支付数)
    val payNum: RDD[(Long, Int)] = payFlatMap.reduceByKey(_ + _)

    // 组合数据
    // (类别，（点击，下单，支付）)
    // cogroup可能存在shuffle过程
    val coGroup: RDD[(Long, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickNum.cogroup(orderNum, payNum)
    val resMap: RDD[(Long, (Int, Int, Int))] = coGroup.mapValues {
      case (clickIter, orderIter, payIter) =>
        val clickNum = if (clickIter.iterator.hasNext) clickIter.iterator.next() else 0
        val orderNum = if (orderIter.iterator.hasNext) orderIter.iterator.next() else 0
        val payNum = if (payIter.iterator.hasNext) payIter.iterator.next() else 0
        (clickNum, orderNum, payNum)
    }
    //排序
    //元组排序：先比较第一个，再比较第二个...
    val sortRdd = resMap.sortBy((tuple: (Long, (Int, Int, Int))) => tuple._2, ascending = false)

    sortRdd.take(10).foreach(println)
  }

  /*
  * 品类倒序：点击数量，下单数量，支付数量
  * 避免shuffle过程，提高效率
  *
  * */
  def hotCategoryTop10_2(dataRdd: RDD[UserVisitData]): Unit = {
    // 缓存公用数据
    dataRdd.cache()

    //点击
    val clickFilter = dataRdd.filter((data: UserVisitData) => data.clickCategoryId != -1)
    val clickGroupBy: RDD[(Long, Iterable[UserVisitData])] = clickFilter.groupBy((data: UserVisitData) => data.clickCategoryId)
    // (cate,(click,0,0))
    val clickNum = clickGroupBy.map((tuple: (Long, Iterable[UserVisitData])) => (tuple._1, (tuple._2.size, 0, 0)))

    // 下单
    val orderFilter = dataRdd.filter((data: UserVisitData) => !data.orderCategoryIds.equals("null"))
    val orderFlatMap = orderFilter.flatMap((data: UserVisitData) => {
      val orderIds = data.orderCategoryIds.split(",")
      // （类别，1）
      orderIds.map((str: String) => (str.toLong, 1))
    })
    val orderNum = orderFlatMap.reduceByKey(_ + _)
    //(cate,(0,order,0))
    val orderNum2 = orderNum.mapValues((i: Int) => (0, i, 0))


    //支付
    val payFilter = dataRdd.filter((data: UserVisitData) => !data.payCategoryIds.equals("null"))
    val payFlatMap = payFilter.flatMap((data: UserVisitData) => {
      val payIds = data.payCategoryIds.split(",")
      // （类别，1）
      payIds.map((str: String) => (str.toLong, 1))
    })
    val payNum = payFlatMap.reduceByKey(_ + _)
    //(cate,(click,0,pay))
    val payNum2 = payNum.mapValues((i: Int) => (0, 0, i))


    // 组合数据
    // (cate,click)=>(cate,(click,0,0))
    // (cate,order)=>(cate,(0,order,0))
    //          聚合=>(cate,(click,order,0))
    // (cate,pay)=>(cate,(click,0,pay))
    //          聚合=>(cate,(click,order,pay))
    // 连接数据
    val allDataRdd = clickNum.union(orderNum2).union(payNum2)
    // (类别，（点击，下单，支付）)
    val resMap = allDataRdd.reduceByKey((t1: (Int, Int, Int), t2: (Int, Int, Int)) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })
    //排序
    val sortRdd = resMap.sortBy((tuple: (Long, (Int, Int, Int))) => tuple._2, ascending = false)

    sortRdd.take(10).foreach(println)
  }


  /*
  * 品类倒序：点击数量，下单数量，支付数量
  * 集中处理，进一步减少shuffle
  *
  * */
  def hotCategoryTop10_3(dataRdd: RDD[UserVisitData]): Unit = {
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

    sortRdd.take(10).foreach(println)
  }
}
