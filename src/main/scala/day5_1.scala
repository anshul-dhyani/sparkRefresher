import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, spark_partition_id}

object day5_1 {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .master("local[1]")
      //If I make it local[*] it created 2 partitions for the inputRDD, why ?
      .appName("Day5_assignments")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

//    Solution 5.1 a
    case class Employee(Name: String, Age: String, Salary: String, City: String)
    //How to override hashcode for case class ?

    val inputRDD = spark.sparkContext.textFile("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\data5_1.csv")
    val header = inputRDD.first()
    val dataRDD = inputRDD.filter(x=>x!=header)

    val rddArray = dataRDD.map(line => line.split(","))
    val employeeRDD = rddArray.map(x => new Employee(x(0),x(1),x(2),x(3)))
    println(s"number of partitions in employee RDD ${employeeRDD.getNumPartitions}")

    val pairedRDD = employeeRDD.map(emp => (emp.City, emp))
    val partitionedByCityRDD = pairedRDD.partitionBy(new CustomPartitioner(2))

//    Solution 5.1 b
    partitionedByCityRDD.foreachPartition( p => {
      println("partition : ")
      p.foreach(print)
      println()
    })

//    Solution 5.1 c

    val inputDF = spark.read
                        .format("csv")
                        .option("delimeter", ",")
                        .option("header",true)
                        .load("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\data5_1.csv")
    inputDF.show()
    println(s"Number of partition in DF : ${inputDF.rdd.getNumPartitions}")

    val inputDS = inputDF.repartition(2, col("City"))

    println(s"Number of partition in DF after partitioning : ${inputDS.rdd.getNumPartitions}")

//    import spark.implicits._
//    val DfWithPartitionNumber = inputDF.withColumn("PartitionNumber", spark_partition_id())
//    DfWithPartitionNumber.show()
//    How to add a new column which shows partition Number ?
    val x = inputDS.rdd.mapPartitionsWithIndex((index,iterator)=>{
      iterator.map(x=>(index,x))
    })

    x.foreach(println)

  }
}
