import org.apache.spark.Partitioner

class CustomSalaryRangePartitioner() extends Partitioner{
  override def numPartitions: Int = 2

   override def getPartition(key: Any): Int = {
    if (key.toString.toLong<=30000) 0 else 1
  }

}
