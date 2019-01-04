package controllers

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ScalaTest {
  def main(args: Array[String]) {

    println("Hello World")

    /*val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g")
    //val conf = new SparkConf().setAppName("SparkApp")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "+")
    val rddFile = sc.textFile("test.txt")
    val rddTransformed = rddFile.map(eachLine => eachLine.split(";;"))
    //Ok("test file->"+rddTransformed.take(3))
    //cleanly shutdown
    sc.stop()*/
    //}

  }
}