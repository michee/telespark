package com.telia.spark


import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.hadoop.fs.Path


object CountDevices extends SparkJob {

  val sparkAppName = "CountDevices"

  // ENV VARIABLES:
  val sparkMaster = Option(System.getenv("SPARK_MASTER")).getOrElse("local")
  val sparkHome   = Option(System.getenv("SPARK_HOME")).getOrElse(".")

  val inputPath   = System.getenv("INPUT_PATH")
  val outputPath  = System.getenv("OUTPUT_PATH")


  val techResultDir = "deviceCount"
  val freqResultDir = "frequencyCount"


  conf
    .setMaster(sparkMaster)
    .set("spark.executor.memory", "1g")
    .set("spark.cores.max", "2")


  def read(ss: SparkSession, resource: String, schema: StructType): DataFrame = {
    ss.read
      .schema(schema)
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv(resource)
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

  def mergeDF(a: DataFrame, b: DataFrame): DataFrame = a.union(b)


  def main(args: Array[String]) = {

    println("STARTING")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    println("RUNNING..")

    val archiveIn  = new ArchiveClient(spark, root = inputPath)
    val archiveOut = new ArchiveClient(spark, root = outputPath)

    val countLogic = new CountLogic(spark)


    /*/ TEST SPARK READ Partition Discovery
    val df = readPartiotioned(spark, "./Archive/lte")
    df.printSchema()
    df.show()
    */

    /*/ TEST Write
    val df = readPartiotioned(spark, "./Archive/lte")
    df.printSchema()
    df.show()
    archive.write("result", df)
     */

    val technologies: List[String] = List("gsm", "umts", "lte")

    val archiveUtil = new ArchiveUtil(archiveIn, archiveOut)

    val techResultMissingDays = archiveUtil.getMissingDays(technologies, techResultDir)
    val freqResultMissingDays = archiveUtil.getMissingDays(technologies, freqResultDir)

    val daysToDo = techResultMissingDays ++ freqResultMissingDays

    daysToDo.foreach { day =>

      println("DAY: " + day)

      val allTechFilesOnADay: List[(String, List[Path])] = technologies.map(tech => (tech, archiveIn.getSingleCSVFile(tech, day)))

      val cellList: List[DataFrame] =
        allTechFilesOnADay
          .filter(_._2.nonEmpty)  // Filter out Technologies that dont have files on the day
          .map{ case (tech, file) =>

            val df = read(spark, file.head.toString, InputSchema.cellSchema)
            df.withColumn("technology", functions.lit(tech))
          }

      val cells: DataFrame = cellList.reduce(mergeDF)

      val siteFile = archiveIn.getSingleCSVFile("site", day)
      val sites: DataFrame = read(spark, siteFile.head.toString, InputSchema.siteSchema)

      // TODO: Check that the date is correct,


      if (techResultMissingDays.contains(day)) {

        val deviceCount = countLogic.countTech(cells, sites)
        val df = countLogic.applyDate(deviceCount, day).coalesce(1)
        archiveOut.write(techResultDir, df)

      }

      if (freqResultMissingDays.contains(day)) {
        val bandCount = countLogic.bandsPerSite(cells, sites)
        val df = countLogic.applyDate(bandCount, day).coalesce(1)
        archiveOut.write(freqResultDir, df)
      }
    }

    println("THE END")

    archiveIn.close()
    archiveOut.close()
    spark.stop()
    System.exit(0)
  }
}
