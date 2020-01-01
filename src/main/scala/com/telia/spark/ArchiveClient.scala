package com.telia.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.fs.{LocatedFileStatus, Path, RemoteIterator}

import scala.annotation.tailrec



class ArchiveUtil(in: ArchiveClient, out: ArchiveClient) {

  def getMissingDays(input: List[String], result: String): Set[Day] = {
    val inputDays: List[Set[Day]] = input.map(in.getAllDaysFromDir)
    val allDays: Set[Day] = inputDays.reduce(_ ++ _)

    val resultDays = out.getAllDaysFromDir(result)

    val missingResult = allDays -- resultDays

    missingResult
  }

}


/**
 * Client to access archived data
 * Could be HDFS, S3 or some other source,
 * in this case. local File system
 **/

class ArchiveClient(ss: SparkSession, root: String) {

  private val fs = org.apache.hadoop.fs.FileSystem.get(ss.sparkContext.hadoopConfiguration)
  private val rootPath: Path = new Path(root)

  // Enable writing only some partitions (days), in partition /year/month/day
  ss.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

  def close() = {
    fs.close()
  }

  @tailrec
  private def listFiles(it: RemoteIterator[LocatedFileStatus], files: List[LocatedFileStatus]): List[LocatedFileStatus] =
    if (!it.hasNext) files
    else {
      val n = it.next()
      listFiles(it, files ::: List(n))
    }


  def listCVSFiles(dir: String, underRoot: Boolean = true): List[LocatedFileStatus] = {

    val dirPath = new Path(rootPath + "/" + dir)

    if (!fs.exists(dirPath))
      return List.empty

    // find All .cvs Files in targetDir
    val it = fs.listFiles(dirPath, true)
    val cvsFiles = listFiles(it, List.empty)

    cvsFiles.filter(_.getPath.getName.endsWith(".csv"))
  }

  def getAllDaysFromDir(dir: String): Set[Day] = {
    val cvsFiles = listCVSFiles(dir)

    // Extract year, month, day from filePath .../year=2019/month=10/day=10/somfile.cvs
    cvsFiles.map { cvsFile =>
      val day   = cvsFile.getPath.getParent
      val month = day.getParent
      val year  = month.getParent

      val d = day.getName.replace("day=", "").toInt
      val m = month.getName.replace("month=", "").toInt
      val y = year.getName.replace("year=", "").toInt

      Day(y, m, d)
    }.toSet
  }

  def toUrl(d: Day): String = s"/year=${d.year}/month=${d.month}/day=${d.day}/"

  def getSingleCSVFile(dirName: String, day: Day): List[Path] = {
    val dayPath = dirName + toUrl(day)
    val files = listCVSFiles(dayPath)
    files.map(_.getPath)
  }

  def write(dir: String, df: DataFrame) = {

    val targetPath = new Path(rootPath + "/" + dir)

    df
      .write
      .partitionBy("year", "month", "day")
      // .mode(SaveMode.ErrorIfExists)
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save(targetPath.toString)
  }

}
