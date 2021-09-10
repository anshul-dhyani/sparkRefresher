import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object day3 {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("TestApp")
      .config("spark.hadoop.validateOutputSpecs", false) //to avoid failure for second or more execution of solution for 2.3.a
      .getOrCreate()

    //    solution 3.1 a
    val inputRDD = spark.sparkContext.textFile("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\data.csv")
    inputRDD.foreach(print)

    //    solution 3.1 b
    val dfWithoutSchema = spark
      .read
      .format("csv")
      .option("delimiter", ";")
      //      .option("header", true)
      .load("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\data.csv")

    dfWithoutSchema.printSchema()
    dfWithoutSchema.show()

    //    solution 3.1 c
    val customSchema = StructType(
      Array(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        //        StructField("dob", TimestampType, true),
        StructField("dob", StringType, true),
        StructField("salary", LongType, true),
        StructField("designation", StringType, true),
        StructField("managerid", IntegerType, true),
        StructField("address", StringType, true),
        StructField("hobbies", StringType, true)
        // what is difference between string and stringtype -- stringType is from org.apache.spark.sql.type, so mostly it is for SQL
        // how to give structType of custom type for example address, time
        // how to give list type
        // what is the third option true -- its nullable or not
      )
    )
    val dfWithCustomSchema = spark.read
      .format("csv")
      .option("delimiter", ";")
      .option("header", true)
      .schema(customSchema)
      .load("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\data.csv")

    dfWithCustomSchema.printSchema()
    dfWithCustomSchema.show()
    println(dfWithCustomSchema.getClass)

    //    Solution 3.2
    import spark.implicits._

    val dataSetWithEmployeeCaseClass = spark.read
      .format("csv")
      .option("delimiter", ";")
      .option("header", true)
      .schema(customSchema)
      .load("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\data.csv").as[Employee]

    dataSetWithEmployeeCaseClass.printSchema()
    dataSetWithEmployeeCaseClass.show()
    print(dataSetWithEmployeeCaseClass.getClass)

  }
}
