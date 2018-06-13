package com.entrobus

import java.io.{File, FileOutputStream}

import breeze.numerics.log

import scala.sys.process._

object LoadPython {

  def CompressFiles(){
    val fileInjar = "hello01.py"   //打包后，可以查看这个脚本在jar的相对路径(我的是存放在根目录)
//    C:\Users\brandon\IdeaProjects\FDDCprocess\src\main\python\hello.py
    val in = this.getClass.getResourceAsStream(fileInjar) //获取脚本InputStream

    //获取jar所在的集群路径
//    println("jarpath is : " + this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath)
    val jarPath = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath.replace("\\","/")
    var start = 0
    if (jarPath.charAt(0).equals('/')) {
      start = 1
    }
    val pyDir = jarPath.substring(start,jarPath.lastIndexOf("/"))+"hello01.py"
    println(pyDir)
    if(in != null){
      val f = new File(pyDir)
      if (!f.exists()) f.mkdirs
      val localFile = pyDir +"hello.py"
      val out = new FileOutputStream(localFile)
      val buf = new Array[Byte](1024)
      try {
        var nLen = in.read(buf)
        while(nLen != -1){
          out.write(buf,0,nLen)
          nLen = in.read(buf)
        }

      }catch {
//        case e:Exception => log.error(e.getMessage)
//        case _ => log.error("Read CompressFile.py Exception")
        case e:Exception => println("ERROR________________")
        case _ => println("ERROR________________")
      }finally{
        in.close
        out.close
      }

      //以上代码便可以将jar里面的脚本写入到了jar包所在集群里面的某台机器的本地路径了，
      //Python可以找到脚本解析，，这样只要把scala那条命令执行就行了
      val para = "args"
      s"python $localFile  $para" !   //para参数

    }else{
//      log.error("a NULL error occurred when Read CompressFile.py in jar,maybe the path is invalid!")
      println("a NULL error occurred when Read CompressFile.py in jar,maybe the path is invalid!")
    }
  }

}
