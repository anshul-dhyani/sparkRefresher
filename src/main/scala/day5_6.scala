import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object day5_6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Assignment 5.6")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

//    Solution 5.6 a
    val taxRates = Map(10000 -> 10, 20000 -> 20, 30000 -> 30)
    val broadcastTaxRates = spark.sparkContext.broadcast(taxRates)

//    Solution 5.6 b
    val dsEmployee = spark.read
      .format("csv")
      .option("delimiter", ";")
      .option("header", true)
      .load("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\data5_3.csv")

    dsEmployee.show()

    //    dsEmployee.withColumn("tax",calculateTax(10))

    def calculateTax(salary: Int): Double = {
      val taxDF = broadcastTaxRates.value //.toSeq.toDF("Salary", "Tax")
      if (salary > 30000) return salary * (taxDF(30000).toDouble / 100)
      if (salary <= 30000 && salary >= 20000) return salary * (taxDF(20000).toDouble / 100)
      if (salary > 10000 && salary <= 20000) salary * (taxDF(10000).toDouble / 100) else 0.0
    }

    val updatedEmp = dsEmployee.rdd.mapPartitions(it => {
      it.map(record => {
        val sal = record.get(3).toString.toInt
        val tax = calculateTax(sal)
        Row.fromTuple(record.get(0), record.get(1), record.get(2), record.get(3), record.get(4), record.get(5), record.get(6), record.get(7), tax)
      })
    })

    updatedEmp.foreach(println)

    val schema = new StructType()
      .add("id", StringType, true)
      .add("Name", StringType, true)
      .add("dob", StringType, true)
      .add("salary", StringType, true)
      .add("designation", StringType, true)
      .add("manager_id", StringType, true)
      .add("address", StringType, true)
      .add("hobbies", StringType, true)
      .add("tax", DoubleType, true)

    val updatedDF = spark.createDataFrame(updatedEmp, schema)
    println(s"updated DF's number of partition : ${updatedDF.rdd.getNumPartitions}")
    updatedDF.show()
  }
}
