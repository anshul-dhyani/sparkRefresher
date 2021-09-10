import org.apache.spark.Partitioner

class CustomPartitioner(override val numPartitions:Int) extends Partitioner{

  override def getPartition(key: Any): Int = {
    Math.abs(key.hashCode())%numPartitions
  }
}
