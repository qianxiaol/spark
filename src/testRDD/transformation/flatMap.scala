package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object flatMap {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List("aa,bb,cc","ddd,eee,fff","gggg,hhhh,iiii")

      val rdd = sc.parallelize(list)

      //map的变换操作是对原RDD中的每个元素进行一对一的操作，生成的RDD中元素的数量与原RDD中元素数量相同，
      //但flatMap可以将每个元素进行一对多的变换操作
      println(rdd.flatMap(_.split(",")).collect().toList)

  }
}
