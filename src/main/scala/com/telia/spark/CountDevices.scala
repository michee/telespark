package com.telia.spark

import org.apache.spark.SparkContext


object CountDevices extends SparkJob {
  val sparkAppName = "CountDevices"


  // ENV VARIABLES:
  val sparkMaster = "local[4]"
  val pathInput   = "."
  val pathOutput  = "."


  def main(args: Array[String]) = {

    println("STARTING")

    conf.setMaster(sparkMaster)
      .set("spark.executor.memory", "1g")
      .set("spark.cores.max", "2")

    val context = new SparkContext(conf)

    println("RUNNING..")


    println("THE END")

    context.stop()
    System.exit(0)
  }
}
