import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object dfWithCustomDelimeter {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("TestApp")
      .config("spark.hadoop.validateOutputSpecs", false) //to avoid failure for second or more execution of solution for 2.3.a
      .getOrCreate()

    import spark.implicits._


//    df_2 = spark.read.text("/FileStore/tables/sample.csv")
//    df_3 = df_2.rdd.map(lambda x : x[0].replace("#", "").replace(",","|")).map(lambda y : y.split("|"))
//    header = df_3.first()
//    new_data = df_3.filter(lambda line : line != header)
//    df_4 = new_data.toDF(header)
//    df_4.show()

//    val df_2 = spark.read.text("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\dataWithCustomDelimeter.csv")
//    val df_3 = df_2.rdd.map( x => x.get(0).asInstanceOf[String].replace("#", "").replace(",","|")).map(y => y.split("|"))
//    val header = df_3.first()
//    val new_data = df_3.filter(line => line != header)
//    val df_4 = new_data.toDF(header:_*)
//    df_4.show()


//    val rdd = spark.sparkContext.textFile("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\dataWithCustomDelimeter.csv")
//    val formattedRDD = rdd.map(x => x.replaceAll("#","").replaceAll(",","|").split('|'))
//    val header = formattedRDD.first()
//    val data = formattedRDD.filter(x => x != header).map(x=> x.mkString("|"))
//    println("header = "+header)
//
//    header.foreach(println)
//    println("data = ")
//    data.foreach(println)
//
//    val df = data.toDF(header : _*)
//    df.printSchema()
//    df.show()


//    val dfData = spark.read.format("csv").option("header",false).load("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\dataWithCustomDelimeter.csv")
//    val properDF = dfData.rdd.map(row => row.get(0).asInstanceOf[String].replaceAll("#","")).map(x=>x.replaceAll(",","|")).map(x=>x.split('|'))
//    val header = properDF.first()
//    println("header : ")
//    header.foreach(println)
//    val dataWithoutHeader = properDF.filter(x => x !=header).flatMap(x=>Seq(person(x(0),x(1),x(2),x(3), x(4)))).toDS()
//    println("data without header : ")
////    dataWithoutHeader.foreach(println)
//    dataWithoutHeader.printSchema()
//    val df = dataWithoutHeader.toDF//(header:_*)
//    df.show()


    val dfData = spark.read.format("csv")
      .option("delimiter", "|#")
      .load("C:\\Users\\Anshul\\Desktop\\learning\\src\\main\\resources\\dataWithCustomDelimeter.csv")
    dfData.show()

    val dfHeader = dfData.rdd.first()
    val header = dfHeader.mkString(",").split(",")
    val newDF = dfData.rdd.map(x => x.mkString(",").split(",")).map(x => person(x(0),x(1),x(2),x(3),x(4)))
    val resultDF = newDF.toDF(header : _*)
    resultDF.show()

//    val ds = spark.createDataset(dataWithoutHeader)
//    val df = ds.toDF(header :_*)
//    df.show()
  }
}

case class person(name:String, age:String, salary:String, city:String ,State:String )
