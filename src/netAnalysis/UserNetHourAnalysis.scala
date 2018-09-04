package netAnalysis

import java.io.StringReader
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object UserNetHourAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("app")
    val sc = new SparkContext(conf)
    val input = sc.textFile("D:\\测试文档\\washedNetDataFile.csv")
    val list=List[Int]().toBuffer
    for(i <- 0 to 23){
      list.append(0)
    }
    val info = input.map{line=>
      for(i<- 0 to 23){
        list(i)=0
      }
      val reader=new CSVReader(new StringReader(line))
      val rn=reader.readNext()
      val sdf=new SimpleDateFormat("yyyy/MM/dd HH:mm")
      val up=sdf.parse(rn(2))
      val down=sdf.parse(rn(3))
      if(up.getDate==down.getDate){//在一天
        for(i<-up.getHours() to down.getHours){
          list(i)=list(i)+1
        }
      }else{//不在一天
        for(i<-up.getHours to 23){
          list(i)=list(i)+1
        }
        for(i<-0 to down.getHours()){
          list(i)=list(i)+1
        }
        for(i<-(up.getDate+1) to (down.getDate-1)){
          for(i<- 0 to 23){
            list(i)=list(i)+1
          }
        }
      }
      val sdf1=new SimpleDateFormat("yyyy-MM-01")
      val date=new Date()
      val calendar=Calendar.getInstance()
      val yearStart=sdf1.format(date)
      var semester=(calendar.get(Calendar.YEAR)-Integer.parseInt(rn(0).substring(0,4)))*2
      if(date.getMonth>6){
        semester=semester+1
      }
      rn(0)+","+yearStart+","+semester -> list.toList
    }
    val hourCounts=info.reduceByKey{(x,y)=>
      val buffer = mutable.Buffer[Int]()
      for(i<- 0 to 23){
        buffer.append(x(i)+y(i))
      }
      buffer.toList
    }
    val result=hourCounts.map{line=>
      val reader=new CSVReader(new StringReader(line._1))
      val rn=reader.readNext()
      var string=rn(0).substring(1)
      for(i<-0 to 23){
        string=string+","+line._2(i)
      }
      string+","+rn(1)+","+rn(2)
    }
    for(data<-result.collect().toList){
      println(data)
      FileUtil.writeFile("D:\\测试文档\\userNetHourAnalysisFile.csv",data+"\n")
    }
  }
}
