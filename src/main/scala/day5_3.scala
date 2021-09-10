import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, RowFactory, SparkSession, functions}

object day5_3 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Assignment 5.3")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //    Solution 5.3 a
    val inputDF = spark.read
      .options(Map("delimiter" -> ";", "header" -> "true", "inferSchema" -> "true"))
      .csv("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\data5_3.csv")

    inputDF.show

    println(s"number of partitions for DF : ${inputDF.rdd.getNumPartitions}")

    val partitionedDF = inputDF.repartition(3)
    println(s"number of partitions for DF after repartitioning : ${partitionedDF.rdd.getNumPartitions}")

    val updatedRDD = partitionedDF.rdd.mapPartitionsWithIndex(
      (index, it) => {
        //        println(s"index : ${index}")
        it.map(record => {
          //          println{s"record ; ${record}"}
          val sal = record.get(3).toString.toInt
          val tax = sal * .1
          Row.fromTuple(record.get(0), record.get(1), record.get(2), record.get(3), record.get(4), record.get(5), record.get(6), record.get(7), tax)
//          RowFactory.create()
        })
      }
    )
    println(s"number of partitions after updation : ${updatedRDD.getNumPartitions}")

    updatedRDD.foreach(println)

    val schema = new StructType()
      .add("id", IntegerType, true)
      .add("Name", StringType, true)
      .add("dob", StringType, true)
      .add("salary", IntegerType, true)
      .add("designation", StringType, true)
      .add("manager_id", IntegerType, true)
      .add("address", StringType, true)
      .add("hobbies", StringType, true)
      .add("tax", DoubleType, true)

    //One should be very careful while doing this if there is any mismatch with the schema it fails to encode
    val updatedDF = spark.createDataFrame(updatedRDD, schema)
    println(s"updated DF's number of partition : ${updatedDF.rdd.getNumPartitions}")
    updatedDF.show()

    val dfWithPartitionNumber = updatedDF.withColumn("partitionNumber", functions.spark_partition_id())
    dfWithPartitionNumber.show()

    //    Solution 5.3 b
    val x = dfWithPartitionNumber.rdd.mapPartitionsWithIndex((index, iterator) => {
      println(s"index : ${index}")
      iterator.map(row => {
        println(s"row/record : ${row}")
        row
      })
    })

    x.foreach(println)
  }

}