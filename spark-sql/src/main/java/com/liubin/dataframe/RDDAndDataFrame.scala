package com.liubin.dataframe

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object RDDAndDataFrame {
  def main(args: Array[String]): Unit = {
    // rdd转换dataFrame
    val config = new SparkConf().setMaster("local[*]").setAppName("PageConversion")
    val sc: SparkContext = new SparkContext(config)
    val sparkSession = SparkSession.builder().master("local[*]").appName("DSL").getOrCreate()
    //StructType and convert RDD to DataFrame
    val schema = StructType(
      Seq(
        StructField("name", StringType, nullable = true)
        , StructField("age", IntegerType, nullable = true)
      )
    )
    val rdd = sc.makeRDD(List(("zhangsan", 12), ("lisi", 23), ("wangwu", 45)))
    val rowRdd = rdd.map(tuple => Row(tuple._1, tuple._2))
    val dataFrame1 = sparkSession.createDataFrame(rowRdd, schema)
    dataFrame1.show()

    //use case class Person
    case class Person(name: String, age: Int)

    //导入隐饰操作，否则RDD无法调用toDF方法
    import sparkSession.implicits._
    val dataFrame2 = rdd.toDF()
    dataFrame2.show()

    sc.stop()
    sparkSession.stop()
  }
}
