import org.apache.spark.sql.{SaveMode, SparkSession}

object day2 {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("TestApp")
      .config("spark.hadoop.validateOutputSpecs", false) //to avoid failure for second or more execution of solution for 2.3.a
      .getOrCreate()

    //    //this import is from the sparksession created above
    //    import spark.implicits._
    //
    //    //above import enables toDS and other DS capabilities
    //    val personDS = Seq(Person("Andy", 1)).toDS()
    //    personDS.show()

    val data = List.range(1, 101)
    val inputrdd = spark.sparkContext.parallelize(data)

    //solution 2.1
    inputrdd.take(10).foreach(println)

    //    solution 2.2
    import spark.implicits._
    val inputDF = inputrdd.toDF()
    //    solution 2.2 a
    inputDF.printSchema()
    //    solution 2.2 b
    inputDF.take(10).foreach(print)

    //    solution 2.3 a
    //question what is happening in 2.2 solution that 1 is not squared ?, file saved has correct values from 1

    val squaredRDD = inputrdd.map(x => x * x)
    squaredRDD.saveAsTextFile("C:/Users/Anshul/Desktop/result/abc.csv")

    //    solution 2.3 b
    val squaredRDDAbove1000 = squaredRDD.filter(x => x >= 1000)
    squaredRDDAbove1000.foreach(println)

    //    solution 2.4
    inputDF.write.mode(SaveMode.Overwrite).parquet("C:/Users/Anshul/Desktop/result/abc.parquet")

  }
}
