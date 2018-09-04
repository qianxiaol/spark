package testRDD.action

import org.apache.spark.{SparkConf, SparkContext}

object first {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List((1,"A"),(2,"B"),(3,"C"))

      val rdd = sc.parallelize(list)

      //返回RDD中的第一个元素，不排序。
      println(rdd.first())
  }
}
