package com.entrobus

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object test {

  def main(args: Array[String]) {
    val inputFile =  "E:\\Spark\\Tencent_ad\\preliminary_contest_data\\userFeature_1005.data"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Spark LR on Tencent data")
      .getOrCreate()

//    val textFile = sc.textFile(inputFile)
//    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
//    wordCount.foreach(println)

//    val vec = new DenseVector(1,2,2,3,4,5,5)
    val rdd01 = sc.parallelize(1 to 10)
//    val rdd02 = rdd01.map(t => (t,"xxx", vec))
//    val df01 = spark.createDataFrame(rdd02)
//    df01.printSchema()

    val temp = parseDouble02("xadfkasd")
//    parseDouble("098073") match {            //正确的方式
//      case Some(t)=>println(t)
//      case None=>println(None)
//    }
    println("convert to " +  temp)
    LoadPython.CompressFiles()

    println(!temp.toString.contains("x") && temp.toString.contains("0"))

  }

  def parseDouble02(s:String) = {
    parseDouble(s) match {
      case Some(t)=> t
      case None=> 0.0
    }
  }
  def parseDouble(s: String): Option[Double] = try { Some(s.toDouble) } catch { case _ => None }

}
