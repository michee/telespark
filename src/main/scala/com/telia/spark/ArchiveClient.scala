package com.telia.spark

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

import scala.annotation.tailrec
import scala.collection.immutable.Stream.Empty

// Client to access archived data
// Could be HDFS, S3 or some other source,
// in this case. local File system

class ArchiveClient(ss: SparkSession, root: String) {

  val fs = org.apache.hadoop.fs.FileSystem.get(ss.sparkContext.hadoopConfiguration)

  val rootPath: Path = new Path(root)

  @tailrec
  private def listFiles(it: RemoteIterator[LocatedFileStatus], files: List[LocatedFileStatus]): List[LocatedFileStatus] =
    if (!it.hasNext) files
    else {
      val n = it.next()
      listFiles(it, files ::: List(n))
    }


  def listCVSFiles(dir: String): List[LocatedFileStatus] = { //List[Day] =

   /* println("root")
    fs.listStatus(rootPath).map(_.getPath).foreach(println)

    println("home dir")
    println( fs.getWorkingDirectory.toString )

    println("rootPath")
    println(rootPath.toString)

  */

  //  println( rootPath + "/" + dir)

    val dirPath = new Path(rootPath + "/" + dir)

   // println("dirPath")
   // println(dirPath)

    if (!fs.exists(dirPath))
      return List.empty

    // find All .cvs Files in targetDir
    val it = fs.listFiles(dirPath, true)
    val cvsFiles = listFiles(it, List.empty)

    cvsFiles.filter(_.getPath.getName.endsWith(".csv"))
  }

  def getAllDaysFromDir(dir: String): Set[Day] = {
    val cvsFiles = listCVSFiles(dir)
    //val exp = "/year=([0-9]+)/month=([0-9]+)/day=([0-9]+)/^.*\\.(cvs)".r
    //val pattern = "/year=([0-9]+)/".r

    // println("cvsFiles")
    // cvsFiles.foreach(println)


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

  def getMissingDays(input: List[String], result: String): Set[Day] = {
    val inputDays: List[Set[Day]] = input.map(getAllDaysFromDir)
    val allDays: Set[Day] = inputDays.reduce(_ ++ _)

    val resultDays = getAllDaysFromDir(result)

    val missingResult = allDays -- resultDays
    missingResult
  }

  def toUrl(d: Day): String = s"/year=${d.year}/month=${d.month}/day=${d.day}/"

  def getSingleCSVFile(dirName: String, day: Day): List[Path] = {
    val dayPath = dirName + toUrl(day)
    val files = listCVSFiles(dayPath)
    files.map(_.getPath)
  }

}
