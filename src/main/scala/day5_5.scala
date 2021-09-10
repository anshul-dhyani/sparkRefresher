import org.apache.spark.sql.SparkSession

object day5_5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Assignment 5.5")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

//    Solution 5.5
    val rdd15_16 = spark.sparkContext.parallelize(Seq(3465045.0,786692,934104,18830227,531,24016599))
    println(rdd15_16.stats())
    val rdd16_17 = spark.sparkContext.parallelize(Seq(3801670.0,810253,783721,19933739,1584,25330967))
    println(rdd16_17.stats())
    val rdd17_18 = spark.sparkContext.parallelize(Seq(4020267.0,895448,1022181,23154838,1713,29094447))
    println(rdd17_18.stats())
    val rdd18_19 = spark.sparkContext.parallelize(Seq(4028471.0,1112405,1268833,24499777,5388,30914874))
    println(rdd18_19.stats())
    val rdd19_20 = spark.sparkContext.parallelize(Seq(3424564.0,756725,1132982,21032927,6097,26353293))
    println(rdd19_20.stats())
    val rdd20_21 = spark.sparkContext.parallelize(Seq(3062221.0,624939,611171,18349941,3836,22652108))
    println(rdd20_21.stats())
  }

}
