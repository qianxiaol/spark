package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object cartesian {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[3]").setAppName("app")
    val sc=new SparkContext(conf)

    val list1=List(1,2,3,4,5)
    val list2=List(1,2,3,4,5)

    val rdd1=sc.parallelize(list1)
    val rdd2=sc.parallelize(list2)
    //两个RDD进行笛卡尔积合并
    rdd1.cartesian(rdd2).collect().toList.foreach(println)
  }
}
