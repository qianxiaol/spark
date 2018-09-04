package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object subtractByKey {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list1 = List(("A"->"1"),("B"->"2"),("C"->"3"))
      val list2 = List(("A"->"a"),("C"->"c"),("D"->"d"))

      val rdd1 = sc.parallelize(list1)
      val rdd2 = sc.parallelize(list2)

      //返回key在RDD中出现，并且不在otherRDD中出现的元素，不去重。
      println(rdd1.subtractByKey(rdd2).collect().toList)
  }
}
