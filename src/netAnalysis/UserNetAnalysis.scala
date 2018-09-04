package netAnalysis

import java.io.StringReader
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object UserNetAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("app")
    val sc = new SparkContext(conf)
    val input = sc.textFile("D:\\测试文档\\washedNetDataFile.csv")
    val info = input.map{line=>
      val reader=new CSVReader(new StringReader(line))
      val rn=reader.readNext()
      val sdf=new SimpleDateFormat("yyyy/MM/dd HH:mm")
      val up=sdf.parse(rn(2))
      val down=sdf.parse(rn(3))
      var count=0
      var time=(down.getTime-up.getTime)/3600000
      if(up.getDay==down.getDay){
        if(down.getHours==23&&down.getMinutes>30){
          count=count+1
        }
      }else{
        count=count+1
      }
      val sdf1=new SimpleDateFormat("yyyy-MM-01")
      val date=new Date()
      val calendar=Calendar.getInstance()
      val yearStart=sdf1.format(date)
      var semester=(calendar.get(Calendar.YEAR)-Integer.parseInt(rn(0).substring(0,4)))*2
      if(date.getMonth>6){
        semester=semester+1
      }
      rn(0)+","+rn(1)+","++yearStart+","+semester-> List(count,time)
    }
    val list=info.reduceByKey{(x,y)=>
      val buffer = mutable.Buffer[Long]()
      for(i<- 0 to 1){
        buffer.append(x(i)+y(i))
      }
      buffer.toList
    }
    val result=list.map{line=>
      val reader=new CSVReader(new StringReader(line._1))
      val rn=reader.readNext()
      rn(0).substring(1)+","+rn(1)+","+rn(2)+","+rn(3)+","+(line._2(0)*0.7+line._2(1)*0.3)
    }
    for(data <- result){
      println(data)
      FileUtil.writeFile("D:\\测试文档\\userNetAnalysisData.csv",data+"\n")
    }
  }
}
