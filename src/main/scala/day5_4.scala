import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}

object day5_4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Assignment 5.4")
      .getOrCreate()

//    val myObjEncoder = org.apache.spark.sql.Encoders.kryo[EmployeeWithHobbies]
//
//    val inputRDD = spark.sparkContext
//      .textFile("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\data5_3.csv")
//
//    val header = inputRDD.first()
//    val rddData = inputRDD.filter(x => x!=header)
//    val rddEmployeeWithHobbies = rddData
//      .map(x => x.split(";"))
//      .map(record => new EmployeeWithHobbies(record(0),record(1),record(2),record(3),record(4),record(5),record(6),record(7)))
////      .map(row=>row.mkString(","))
//
//    rddEmployeeWithHobbies.foreach(println)
//
//    val schema = new StructType()
//      .add("id", StringType, true)
//      .add("Name", StringType, true)
//      .add("dob", StringType, true)
//      .add("salary", StringType, true)
//      .add("designation", StringType, true)
//      .add("manager_id", StringType, true)
//      .add("address", StringType, true)
//      .add("hobbies", StringType, true)
//
////    val employeeDF = spark.createDataFrame(rddEmployeeWithHobbies)
////    employeeDF.show()
//    val dsEmployeeWithHobbies = spark.createDataset[EmployeeWithHobbies](rddEmployeeWithHobbies,schema)//(myObjEncoder)
//    dsEmployeeWithHobbies.printSchema()


    //    Solution 5.4 a
    import spark.implicits._

    val dsEmployee = spark.read
      .format("csv")
      .option("delimiter", ";")
      .option("header", true)
      .load("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\data5_3.csv").as[EmployeeWithHobbies]

    dsEmployee.show()
  }
}
