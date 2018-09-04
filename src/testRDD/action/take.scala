package testRDD.action

import org.apache.spark.{SparkConf, SparkContext}

object take {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List(1,2,3,4,5)

      val rdd = sc.parallelize(list,3)

      //take用于获取RDD中从0到num-1下标的元素，不排序。
      println(rdd.take(3).toList)
  }
}
