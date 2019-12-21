package com.telia.spark

import java.nio.file.Paths

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StringType, StructField, StructType}


object CountDevices extends SparkJob {
  val sparkAppName = "CountDevices"

  // ENV VARIABLES:
  val sparkMaster = "local[4]"
  val pathInput   = "."
  val pathOutput  = "."

  conf
    .setMaster(sparkMaster)
    .set("spark.executor.memory", "1g")
    .set("spark.cores.max", "2")

  // For implicit conversions like converting RDDs to DataFrames
  // import spark.implicits._

  // year;month;day;cell_identity;frequency_band;site_id
  // 2018;10;24;C;1800;3
  val cellCSV = Seq(
    ("year"           , IntegerType),
    ("month"          , IntegerType),
    ("day"            , IntegerType),
    ("cell_identity"  , StringType),
    ("frequency_band" , IntegerType),
    ("site_id"        , IntegerType),
  )

  // year;month;day;site_id
  // 2018;10;24;3
  val siteCSV = Seq(
    ("year"    , IntegerType),
    ("month"   , IntegerType),
    ("day"     , IntegerType),
    ("site_id" , IntegerType),
  )

  val cellSchema = createSchema(cellCSV)
  val siteSchema = createSchema(siteCSV)

  def createSchema(t: Seq[(String, DataType)]): StructType =
    StructType(
      t.map{case (name, dType) => StructField(name, dType, nullable = false)}
    )

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  def read(ss: SparkSession, resource: String, schema: StructType): DataFrame = {
    val df = ss.read.schema(schema)
      .option("header", "true")
      .option("delimiter", ";")
      .csv(resource)
    df
  }

  // Without Schema
  def read(spark: SparkSession, resource: String) = {
    spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .load(resource)
  }

  def main(args: Array[String]) = {

    println("STARTING")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    println("RUNNING..")


    println("THE END")

    spark.stop()
    System.exit(0)
  }
}
