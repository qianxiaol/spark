package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object coalesce {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[3]").setAppName("app")
    val sc = new SparkContext(conf)
    val list=List(1,2,3,4,5)
    val rdd=sc.parallelize(list,4)
    println(rdd.partitions.length)
    //用于将RDD进行重分区，使用HashPartitioner。
    // 且该RDD的分区个数等于numPartitions个数。
    // 如果shuffle设置为true，则会进行shuffle。
    val rdd2=rdd.coalesce(8,true)
    println(rdd2.partitions.length)
  }
}
