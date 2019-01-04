package com.mindtree

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object logg
{
  def main(args: Array[String]): Unit = {
      Logger.getLogger ("org").setLevel (Level.ERROR)
      System.setProperty ("hadoop.home.dir", "D:\\softwares\\Hadoop")
      val conf = new SparkConf ( ).setMaster ("local").setAppName ("logger");
      val sc = new SparkContext (conf);
      val Array(inpath,outpath) = args
      val textFile = sc.textFile (inpath);
      val reg = "\\n|\\t"
      val pat1: String = "ERROR"
      val pat2: String = "Exception"
      val pat3: String = "(?i)(failure)"
      val pat4: String = "Failure"
      val counts = textFile.flatMap (line => (line.split (reg))).filter (line => line.contains (pat1) || line.contains (pat2) || line.contains (pat3) || line.contains (pat4))
      //counts.foreach(println)
      counts.coalesce (1).saveAsTextFile (outpath)
  }
}
