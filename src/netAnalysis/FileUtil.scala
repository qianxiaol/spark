package netAnalysis

import java.io.{File, FileWriter, PrintWriter}

object FileUtil {
  def writeFile(filename:String,input:String) ={
    val file = new File(filename)
    val fw = new FileWriter(file,true)
    val fileWriter=new PrintWriter(fw)
    fileWriter.write(input)
    fileWriter.close()
  }
}
