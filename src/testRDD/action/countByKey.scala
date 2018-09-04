package testRDD.action

import org.apache.spark.{SparkConf, SparkContext}

object countByKey {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List((1,"A"),(2,"B"),(3,"C"),(3,"D"))

      val rdd = sc.parallelize(list)

      //用于统计RDD[K,V]中每个K的数量。
      println(rdd.countByKey())
  }
}
