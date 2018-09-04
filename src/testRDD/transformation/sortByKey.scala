package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object sortByKey {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List((5,90),(1,92),(3,50))
      val rdd = sc.parallelize(list)
      //false降序；true升序
      println(rdd.sortByKey(true).collect().toList)

  }
}
