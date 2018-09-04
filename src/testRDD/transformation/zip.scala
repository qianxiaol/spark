package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object zip {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list1 = List(1,2,3,4,5)
      val list2 = List(6,7,8,9,10)

      val rdd1 = sc.parallelize(list1)
      val rdd2 = sc.parallelize(list2)

      //用于将两个RDD组合成Key/Value形式的RDD,这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常。
      println(rdd1.zip(rdd2).collect().toList)
  }
}
