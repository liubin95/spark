package com.liubin.example

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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

    // 太费劲了，去hive里面写了
    // src/main/resources/spark-sql-example.sql

    // 自定义聚合函数
    sparkSession.udf.register("cityRemark", functions.udaf(new CityRemark))
    sparkSession.sql(
      """
        |SELECT *
        |FROM (
        |         SELECT *,
        |                ROW_NUMBER() OVER (PARTITION BY area ORDER BY area_sum DESC ) AS area_rank
        |         FROM (
        |                  SELECT
        |                      t1.area,
        |                      t1.p_name,
        |                      COUNT(*) AS area_sum,
        |                      cityRemark(c_name) AS city_remark
        |                  FROM (
        |                           SELECT
        |                               u.*,
        |                               c.name AS c_name,
        |                               c.area,
        |                               p.name AS p_name
        |                           FROM user_visit u
        |                           JOIN city       c ON c.id = u.cityId
        |                           JOIN prod       p ON p.id = u.clickProductId
        |                           WHERE u.clickProductId != -1
        |                           ) t1
        |                  GROUP BY t1.area, t1.p_name
        |                  ) t2
        |         ) t3
        |WHERE t3.area_rank <= 3
        |""".stripMargin).show()
  }

  case class Buffer(
                     var total: Long,
                     var cityMap: mutable.Map[String, Long]
                   )

  /**
   * 自定义聚合函数
   * in:城市名称
   * buf：[总和点击，Map[(city,sum),(city,sum)]
   * out：备注信息
   * */
  class CityRemark extends Aggregator[String, Buffer, String] {
    // 缓冲初始化
    override def zero: Buffer = {
      Buffer(0, mutable.Map[String, Long]())
    }

    // 更新buffer
    override def reduce(b: Buffer, a: String): Buffer = {
      // 总数
      b.total += 1
      // 城市的点击数量
      val newCnt = b.cityMap.getOrElse(a, 0L) + 1
      b.cityMap.update(a, newCnt)
      b
    }

    // 合并buffer
    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.total += b2.total
      // map合并
      val map1 = b1.cityMap
      val map2 = b2.cityMap
      b1.cityMap = map1.foldLeft(map2) {
        case (map, (city, sum)) =>
          val newCnt = map.getOrElse(city, 0L) + sum
          map.update(city, newCnt)
          map
      }
      b1
    }

    // 生成统计结果
    override def finish(reduction: Buffer): String = {
      val strings = ListBuffer[String]()
      // 城市集合排序
      var other = 0.0
      reduction.cityMap.toList
        .sortBy(_._2)(Ordering.Long.reverse).take(2).foreach((tuple: (String, Long)) => {
        val value = tuple._2 * 100 / reduction.total
        other += value
        strings.append(f"${tuple._1}:$value%%")
      })

      val hasMore = reduction.cityMap.size > 2
      if (hasMore) {
        strings.append(f"其他:${100 - other}%%")
      }

      strings.mkString(",")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

}
