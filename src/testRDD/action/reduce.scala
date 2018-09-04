package testRDD.action

import org.apache.spark.{SparkConf, SparkContext}

object reduce {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List(1,2,3,4,5,6)

      val rdd = sc.parallelize(list)

      println(rdd.reduce(_+_))

  }
}
