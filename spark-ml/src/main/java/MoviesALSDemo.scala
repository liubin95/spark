import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 *
 *
 *
 * */
object MoviesALSDemo {
  def main(args: Array[String]): Unit = {
    //1.准备环境
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MoviesALSDemo")
      .getOrCreate()
    val sparkContext = spark.sparkContext
    sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val fileDS: Dataset[String] = spark.read.textFile("input/u.data")
    val rateDF = fileDS.map((str: String) => {
      //196	242	3	881250949
      //uid iid rate time
      val arr = str.split("\t")
      (arr(0).toInt, arr(1).toInt, arr(2).toDouble)
    }).toDF("userId", "itemId", "rate")

    //按照8:2划分训练集和测试集
    val Array(trainSet, testSet) = rateDF.randomSplit(Array(0.8, 0.2))

    val als = new ALS()
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rate")
      .setRank(10) //可以理解为Cm*n = Am*k X Bk*n 里面的k的值
      // 梯度下降
      .setMaxIter(10) //最大迭代次数
      .setAlpha(1.0) //迭代步长

    val moviesModel: ALSModel = als.fit(trainSet)

    //使用测试集测试模型
    //    val testResult: DataFrame = moviesModel.recommendForUserSubset(testSet, 5)
    //    testResult.show(false)
    //TODO 计算模型误差--模型评估

    // 给用户做推荐
    val result1: DataFrame = moviesModel.recommendForAllUsers(5) //给所有用户推荐5部电影
    val result2: DataFrame = moviesModel.recommendForAllItems(5) //给所有电影推荐5个用户
    val result3: DataFrame = moviesModel.recommendForUserSubset(sparkContext.makeRDD(Array(196)).toDF("userId"), 5) //给指定用户推荐5部电影
    val result4: DataFrame = moviesModel.recommendForItemSubset(sparkContext.makeRDD(Array(242)).toDF("itemId"), 5) //给指定电影推荐5个用户
    result1.show(false)
    result2.show(false)
    result3.show(false)
    result4.show(false)
  }

}
