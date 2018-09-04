package testRDD.action

import org.apache.spark.{SparkConf, SparkContext}

object count {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List((1,"A"),(2,"B"),(3,"C"))
      val rdd = sc.parallelize(list)
      //返回RDD中的元素数量。
      println(rdd.count())
  }
}
