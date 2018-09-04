package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object subtract {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list1 = List(1,2,2,3,3,4,5,6,7,9)
      val list2 = List(2,4,5,6,7)

      val rdd1 = sc.parallelize(list1)
      val rdd2 = sc.parallelize(list2)

      //返回在RDD中出现，并且不在otherRDD中出现的元素，不去重。
      println(rdd1.subtract(rdd2).collect().toList)
  }
}
