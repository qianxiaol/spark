package testRDD.action

import org.apache.spark.{SparkConf, SparkContext}

object countByValue {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List((1,"A"),(2,"B"),(3,"C"),(3,"C"),(4,"C"))

      val rdd = sc.parallelize(list)

      //统计出集合中每个元素的个数
      println(rdd.countByValue())
  }
}
