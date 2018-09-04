package testRDD.action

import org.apache.spark.{SparkConf, SparkContext}

object takeSample {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List(1,2,3,4,5,6,7,8,9,10)

      val rdd = sc.parallelize(list,3)

      println(rdd.takeSample(false,4,9).toList)
  }
}
