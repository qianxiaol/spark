package testRDD.action

import org.apache.spark.{SparkConf, SparkContext}

object saveAsTextFile {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List(1,3,5,2,4)

      val rdd = sc.parallelize(list,3)

      //将数据集的元素，以textfile的形式保存到本地文件系统hdfs或者任何其他hadoop支持的文件系统，spark将会调用每个元素的toString方法，并将它转换为文件中的一行文本
      rdd.saveAsTextFile("D://test2")

      //使用java的序列化方法保存到本地文件，可以被sparkContext.objectFile()加载
      rdd.saveAsObjectFile("D://test1")
  }
}
