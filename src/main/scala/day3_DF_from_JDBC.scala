import org.apache.spark.sql.{SaveMode, SparkSession}

object day3_DF_from_JDBC {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("TestApp")
      .config("spark.hadoop.validateOutputSpecs", false) //to avoid failure for second or more execution of solution for 2.3.a
      .getOrCreate()

    //    Solution 3.3 a
    val jdbcDFWithoutData = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://db4free.net:3306/anshuldemospark")
      .option("dbtable", "employee")
      .option("user", "anshuldhyani")
      .option("password", "password@123")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()

    jdbcDFWithoutData.show()

    //    Solution 3.3 b
    val dfWithCustomSchema = spark.read
      .format("csv")
      .option("delimiter", ";")
      .option("header", true)
      .load("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\data.csv")

    dfWithCustomSchema.show()
    dfWithCustomSchema.write
      .format("jdbc")
      .option("url", "jdbc:mysql://db4free.net:3306/anshuldemospark")
      .option("dbtable", "employee")
      .option("user", "anshuldhyani")
      .option("password", "password@123")
      .option("driver", "com.mysql.jdbc.Driver")
      .mode(SaveMode.Append) //to avoid creating new table
      .save()

    //    Solution 3.3 c

    val jdbcDFWithData = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://db4free.net:3306/anshuldemospark")
      .option("dbtable", "employee")
      .option("user", "anshuldhyani")
      .option("password", "password@123")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()

    jdbcDFWithData.show()
    jdbcDFWithData.printSchema()
  }
}
