import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object day4 {
  def main(args: Array[String]): Unit = {

    //    Solution 4.1 a
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Day4_assignments")
      .getOrCreate()

    import spark.implicits._

    val data = List.range(1, 1001)
    val intRDD = spark.sparkContext.parallelize(data)
    println("Number of partitions when RDD : " + intRDD.getNumPartitions)

    val intDF = intRDD.toDF()
    intDF.show(5)
    println("Number of partitions when DF : " + intDF.rdd.getNumPartitions)

    val repartitionedIntDF = intDF.repartition(3)
    intDF.show(5)
    println("Number of partitions after repartitioning : " + repartitionedIntDF.rdd.getNumPartitions)


    //    Solution 4.1 b1

    val customSchema = StructType(
      Array(
        StructField("Name", StringType, true),
        StructField("Age", IntegerType, true),
        StructField("Salary", LongType, true),
        StructField("City", StringType, true),
      )
    )

    val customSchemaDF = spark.read
                          .format("csv")
                          .schema(customSchema)
                          .option("header", true)
                          .load("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\data2.csv")

    customSchemaDF.show()

    println("Number of partitions for customSchemaDF : "+customSchemaDF.rdd.getNumPartitions)

//    Solution 4.1 b2
    val cityWiseDS = customSchemaDF.groupBy("City")
    val cityWiseSalaryDF = cityWiseDS.sum("Salary")
    cityWiseSalaryDF.show()
    println("Number of partitions for cityWiseSalaryDF : "+cityWiseSalaryDF.rdd.getNumPartitions)


    //    Solution 4.1 b3
    cityWiseSalaryDF.explain
//    Can see the plan but cannot understand it

    //    Solution 4.1 b4
    //    In job page it shows Stages Succeeded/Total as 2/2
    //    In Stage page it shows DAG and other details about the shuffle


//    Solution 4.2 a
    println("Number of partitions before repartition : "+customSchemaDF.rdd.getNumPartitions)

    val repartitionedDF = customSchemaDF.repartition(col("City"))
    println("Number of partitions after repartition : "+repartitionedDF.rdd.getNumPartitions)

//    Solution 4.2 b
    val mapPartitionedDF = customSchemaDF.mapPartitions(

      iterator => {
        // Do the heavy initialization here
        // Like database connections e.t.c

        val result = iterator.map(row=>{
          (row.getString(3),row.getLong(2))
        })
        result
      }
    )

    println("number of partitions for MapPartitionedDF : "+mapPartitionedDF.rdd.getNumPartitions)
    val groupedDF = mapPartitionedDF.groupBy("_1").sum()
    println("number of partitions for groupedDF : "+groupedDF.rdd.getNumPartitions)
    val namedDF = groupedDF.toDF("City","Salary")
    println("number of partitions for namedDF : "+namedDF.rdd.getNumPartitions)

    namedDF.show()

  }
}
