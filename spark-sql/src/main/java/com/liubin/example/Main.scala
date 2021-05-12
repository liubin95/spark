package com.liubin.example

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  // 数据结构
  case class City(id: Int, name: String, area: String)

  case class Prod(id: Int, name: String, sellType: String)

  case class UserVisitData(
                            date: String,
                            userId: String,
                            sessionId: String,
                            pageId: Long,
                            actionTime: String,
                            searchKeyWord: String,
                            clickCategoryId: Long,
                            clickProductId: Long,
                            orderCategoryIds: String,
                            orderProductIds: String,
                            payCategoryIds: String,
                            payProductIds: String,
                            cityId: Long
                          )

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("Main").getOrCreate()
    val config = new SparkConf().setMaster("local[*]").setAppName("main")
    val sc = SparkContext.getOrCreate(config)
    import sparkSession.implicits._

    val city = sc.textFile("input/sql/city_info.txt")
    val prod = sc.textFile("input/sql/product_info.txt")
    val userVisit = sc.textFile("input/sql/user_visit_action.txt")

    // 数据源
    val cityDS = city.map((str: String) => {
      val strings = str.split("\t")
      City(strings(0).toInt, strings(1), strings(2))
    }).toDS()
    val prodDS: Dataset[Prod] = prod.map((str: String) => {
      val strings = str.split("\t")
      Prod(strings(0).toInt, strings(1), strings(2))
    }).toDS()
    val userVisitDS: Dataset[UserVisitData] = userVisit.map((str: String) => {
      val strings = str.split("\t")
      UserVisitData(
        strings(0),
        strings(1),
        strings(2),
        strings(3).toInt,
        strings(4),
        strings(5),
        strings(6).toInt,
        strings(7).toInt,
        strings(8),
        strings(9),
        strings(10),
        strings(11),
        strings(12).toInt
      )
    }).filter(_.clickProductId != -1).toDS()

    cityDS.printSchema()
    prodDS.printSchema()
    userVisitDS.printSchema()

    // 创建临时表
    cityDS.createOrReplaceTempView("city")
    prodDS.createOrReplaceTempView("prod")
    userVisitDS.createOrReplaceTempView("user_visit")


    sparkSession.sql(
      """
        |select count(*) from user_visit
        |""".stripMargin).show()

    // 各个区域的热门商品（点击量）前三名
    // 并备注每个商品在主要城市种的分布比例
    // 华北 商品A 1000  北京20%，天津10%，其他70%

    // 城市 - 商品 - 点击数量
    sparkSession.sql(
      """
        |select cityId,clickProductId,count(*) as num
        |from user_visit
        |group by cityId,clickProductId
        |""".stripMargin).show()

    sparkSession.sql(
      """
        |select *,row_number() over(partition by cityId order by t1.num desc) as rank from (
        |select cityId,clickProductId,count(*) as num
        |from user_visit
        |group by cityId,clickProductId ) t1
        |""".stripMargin).show()
    // 城市热门商品 top3
    sparkSession.sql(
      """
        |select * from (
        |select *,row_number() over(partition by cityId order by t1.num desc) as rank from (
        |select cityId,clickProductId,count(*) as num
        |from user_visit
        |group by cityId,clickProductId ) t1 ) t2
        |where t2.rank < 4
        |""".stripMargin).show()

    // 连接city
    sparkSession.sql(
      """
        |select t2.*,t3.* from (
        |select *,row_number() over(partition by cityId order by t1.num desc) as rank from (
        |select cityId,clickProductId,count(*) as num
        |from user_visit
        |group by cityId,clickProductId ) t1 ) t2
        |join city t3 on t3.id  = t2.cityId
        |where t2.rank < 4
        |""".stripMargin).show()

    // 地区分类
    sparkSession.sql(
      """
        |select *, row_number() over(partition by area order by t4.num) as area_rank from (
        |select t2.*,t3.* from (
        |select *,row_number() over(partition by cityId order by t1.num desc) as rank from (
        |select cityId,clickProductId,count(*) as num
        |from user_visit
        |group by cityId,clickProductId ) t1 ) t2
        |join city t3 on t3.id  = t2.cityId
        |where t2.rank < 4 ) t4
        |""".stripMargin).show()


    // 地区热门top3
    sparkSession.sql(
      """
        |select * from (
        |select *, row_number() over(partition by area order by t4.num desc) as area_rank from (
        |select t2.*,t3.* from (
        |select *,row_number() over(partition by cityId order by t1.num desc) as rank from (
        |select cityId,clickProductId,count(*) as num
        |from user_visit
        |group by cityId,clickProductId ) t1 ) t2
        |join city t3 on t3.id  = t2.cityId
        |where t2.rank < 4 ) t4 ) t5
        |where t5.area_rank < 4
        |""".stripMargin).show()

    // 拼接商品
    sparkSession.sql(
      """
        |select area,t5.name,t6.name from (
        |select *, row_number() over(partition by clickProductId order by t4.num desc) as area_rank from (
        |select t2.*,t3.* from (
        |select *,row_number() over(partition by clickProductId order by t1.num desc) as rank from (
        |select cityId,clickProductId,count(*) as num
        |from user_visit
        |group by cityId,clickProductId ) t1 ) t2
        |join city t3 on t3.id  = t2.cityId
        |where t2.rank < 4 ) t4 ) t5
        |join prod t6 on t5.clickProductId = t6.id
        |where t5.area_rank < 4
        |""".stripMargin).show()

    // 太费劲了，去hive里面写了
    // src/main/resources/spark-sql-example.sql
  }

}
