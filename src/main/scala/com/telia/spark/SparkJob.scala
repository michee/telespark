package com.telia.spark

import org.apache.spark.SparkConf
// import org.apache.logging.log4j.scala.Logging

trait SparkJob {// extends Logging {

  def sparkAppName: String

  // logger.info(s"Starting $this with Xmx${Runtime.getRuntime.maxMemory/1024L/1024L}M")

  lazy val conf =
    new SparkConf()
      .setAppName(sparkAppName)
      .setSparkHome(Option(System.getenv("SPARK_HOME")).getOrElse("."))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.cleaner.ttl", "3600")
      .set("spark.executor.memory", "256m")
      .set("spark.ui.port", sys.env.getOrElse("SPARK_UI_PORT", "4040"))

}