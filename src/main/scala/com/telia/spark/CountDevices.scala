package com.telia.spark

import java.nio.file.Paths

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions
import org.apache.hadoop.fs.Path


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
    val df = ss.read
      .schema(schema)
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv(resource)
    df
  }

  // Read Partitioned
  def readPartiotioned(ss: SparkSession, dir: String): DataFrame = {
    ss.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .format("com.databricks.spark.csv")
      .load(dir)
  }

  // ROOT
  //def getCellData(ss: SparkSession, technology: String): DataFrame =


  def merge(a: DataFrame, b: DataFrame): DataFrame = a.union(b)

  def main(args: Array[String]) = {

    println("STARTING")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    println("RUNNING..")


    /*/ TEST SPARK READ Partition Discovery
    val df = readPartiotioned(spark, "./Archive/lte")
    df.printSchema()
    df.show()
    */

    val archive = new ArchiveClient(spark, "./Archive")

    val technologies: List[String] = List("gsm", "umts", "lte")
    val daysToDo = archive.getMissingDays(technologies, "result")
    println(daysToDo)

    daysToDo.foreach { day =>

      println("DAY: " + day)

      val techFiles: List[(String, List[Path])] = technologies.map(tech => (tech, archive.getSingleCSVFile(tech, day)))

      val cellList: List[DataFrame] =
        techFiles
          .filter(_._2.nonEmpty)  // Filter out Technologies that dont have files on the day
          .map{ case (tech, file) =>
            val df = read(spark, file.head.toString, cellSchema)
            df.withColumn("technology", functions.lit(tech))
          }

      val cells: DataFrame = cellList.reduce(merge)

      val siteFile = archive.getSingleCSVFile("site", day)
      val sites: DataFrame = read(spark, siteFile.head.toString, siteSchema)

      // val deviceCount = new CountLogic(spark).countTech(cells, sites)

      val bandCount = new CountLogic(spark).bandsPerSite(cells, sites)

    }

    println("THE END")

    spark.stop()
    System.exit(0)
  }
}
