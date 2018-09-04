package netAnalysis

import java.io.StringReader
import java.text.SimpleDateFormat
import java.util.Date

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object DataClean {
  def isNullOrNone(str:String) ={
    var temp=true
    var line=str.split(",").toList
    for(i <- line){
      if(None == i || null == i){
        temp=false
      }
    }
    var sdf=new SimpleDateFormat("yyyy/MM/dd HH:mm")
    val date1=sdf.parse(line(1))
    val date2=sdf.parse(line(2))
    if(date1.compareTo(date2)>0){
      println(line(1)+"===="+line(2))
      temp=false
    }
    temp
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("app")
    val sc = new SparkContext(conf)
    //获取性别文件的内容，将学号和性别存到Map中
    val infoList = Source.fromFile("D:\\测试文档\\spark\\sexDictFile.csv").getLines().toList
    val infoMapBuffer = Map[String,String]().toBuffer
    for(line <- infoList){
      val info = line.toString.split(",").toList
      if(info(1)=="1"||info(1)=="2")infoMapBuffer += (info(0) -> info(1))
    }
    val infoMap = infoMapBuffer.toMap

    val input =sc.textFile("D:\\测试文档\\spark\\netClean.csv").filter(isNullOrNone)
    val result=input.map{line =>
      val reader=new CSVReader(new StringReader(line))
      val rn = reader.readNext()
      rn(0)+","+infoMap(rn(0))+","+rn(1)+","+rn(2)
    }
    val cleanedData=result.collect().toList
    for(data <- cleanedData){
      println(data)
      FileUtil.writeFile("D:\\测试文档\\washedNetDataFile.csv",data+"\n")
    }
  }
}
