package testRDD.action

import org.apache.spark.{SparkConf, SparkContext}

object collectAsMap {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List((1,"A"),(1,"B"),(2,"C"),(2,"D"),(3,"E"))

      val rdd = sc.parallelize(list)

      //如果RDD中同一个Key中存在多个Value，那么后面的Value将会把前面的Value覆盖，
      //最终得到的结果就是Key唯一，而且对应一个Value。
      println(rdd.collectAsMap())

  }
}
