package com.telia.spark

import org.apache.spark.SparkConf

object LocalSparkConf {

  def apply(): SparkConf =
    new SparkConf(false)
      .setMaster("local")
      .set("spark.driver.allowMultipleContexts", "false")
      .set("spark.ui.enabled", "false")
}
