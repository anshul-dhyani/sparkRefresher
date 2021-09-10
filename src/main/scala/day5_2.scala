import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{column, spark_partition_id}

object day5_2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Assignment 5.2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //    Solution 5.2 a
    val inputRDD = spark.sparkContext.textFile("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\data5_2.csv")

    val header = inputRDD.first()
    val inputDataRDD = inputRDD.filter(x => x != header)

    println(s" number of partitions : ${inputDataRDD.getNumPartitions}")

    inputDataRDD.foreach(println)

    val pairedRDD = inputDataRDD.map(x => (x.split(";")(3), x))
    println(s" number of partitions of Paired RDD: ${pairedRDD.getNumPartitions}")
    pairedRDD.foreach(println)

    val rangePartitionedRDD = pairedRDD.partitionBy(new CustomSalaryRangePartitioner)

    println(s" number of partitions after range partitioning : ${rangePartitionedRDD.getNumPartitions}")

    rangePartitionedRDD.foreachPartition(p => {
      println("partition : ")
      p.foreach(print)
      println()
    })

    //    Solution 5.2 b
    val inputDF = spark.read
      .options(Map("inferSchema" -> "true", "delimiter" -> ";", "header" -> "true"))
      .csv("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\data5_2.csv")

    inputDF.show()
    println(s"number of partitions for DF before partitioning : ${inputDF.rdd.getNumPartitions}")

    val partitionedDF = inputDF.repartitionByRange(column("salary"))
    println(s"number of partitions for DF after partitioning : ${partitionedDF.rdd.getNumPartitions}")

    val DfWithPartitionNumber = partitionedDF.withColumn("PartitionNumber", spark_partition_id())
    DfWithPartitionNumber.show()

  }
}
