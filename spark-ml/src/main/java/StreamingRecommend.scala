import com.google.gson.Gson
import domain.Answer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{StreamingContext, Time}

object StreamingRecommend {

  def main(args: Array[String]): Unit = {
    //1.准备环境
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ALSModeling")
      .getOrCreate()
    val sparkContext = spark.sparkContext
    val ssc: StreamingContext = new StreamingContext(sparkContext, streaming.Seconds(5))
    sparkContext.setLogLevel("WARN")
    import spark.implicits._
    // kafka配置信息
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "LAPTOP-MOJU49C3.localdomain:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "spark_group",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // 卡夫卡工具类，泛型为数据key和value
    val kafkaData: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      // 采集的节点和计算节点的匹配规则
      // 自动选择
      LocationStrategies.PreferConsistent,
      // 订阅信息
      ConsumerStrategies.Subscribe[String, String](Set("edu"), kafkaParams)
    )

    val valueDStream: DStream[Answer] = kafkaData.map(_.value())
      .map((str: String) => {
        val gson: Gson = new Gson()
        gson.fromJson(str, classOf[Answer])
      })
    valueDStream.foreachRDD((value: RDD[Answer], time: Time) => {
      val model: ALSModel = ALSModel.load("output/als_model")
      val answerDF: DataFrame = value.coalesce(1).toDF
      val studentIdDF = answerDF.select('student_id_int as "student_id")
      val recommendDF: DataFrame = model.recommendForUserSubset(studentIdDF, 10)
      recommendDF.printSchema()
      recommendDF.show(false)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
