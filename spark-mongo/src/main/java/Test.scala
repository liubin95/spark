import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document


object Test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("MongoSparkConnectorIntro")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/liubin.sa_tree")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/liubin.spark_out")
      .getOrCreate()

    // 返回的DataFrame
    val frame: DataFrame = MongoSpark.load(spark)
    println(frame.count())
    println(frame.first())
    // 返回MongoRdd
    val mongoRdd: MongoRDD[Document] = MongoSpark.load(spark.sparkContext)
    // Mongo的过滤方法
    val aggregatedRdd = mongoRdd.withPipeline(Seq(Document.parse("""{ $match:{label_code:"INDUSTRIES_NATIONAL"}}""")))
    println(aggregatedRdd.count)
    println(aggregatedRdd.first.toJson)
    // 写入数据
    val documents = spark.sparkContext.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
    MongoSpark.save(documents)
    spark.close()
  }


}
