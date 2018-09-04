package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object union {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list1 = List(1,(1,2,3))
      val list2 = List(3,(4,5,6))

      val rdd1 = sc.parallelize(list1,2)
      val rdd2 = sc.parallelize(list2,2)

      //合并两个RDD，不去重
      println(rdd1.union(rdd2).collect().toList)
  }
}
