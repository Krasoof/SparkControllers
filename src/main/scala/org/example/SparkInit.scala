package org.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}


object SparkInit extends App {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  val conf = new SparkConf().setMaster("local").setAppName("FirstSparkApi")

  val sc = new SparkContext(conf)

  val lines = sc.textFile("enwik8")

  val x = lines.count();

  println(x);

}
