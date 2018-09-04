package testRDD.action

import org.apache.spark.{SparkConf, SparkContext}

object top {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List(1,3,5,2,4)

      val rdd = sc.parallelize(list,3)
      //用于从RDD中，按照默认（降序）或者指定的排序规则，返回前num个元素。
      println(rdd.top(3).toList)

      //takeOrdered和top类似，只不过以和top相反的顺序返回元素。
      println(rdd.takeOrdered(9).toList)
  }
}
