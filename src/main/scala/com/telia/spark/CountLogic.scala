package com.telia.spark

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._



case class CellRow(
  year: Int,
  month: Int,
  day: Int,
  cell_identity: String,
  frequency_band: Int,
  site_id: Int,
  technology: String
)

case class SiteRow(
  year: Int,
  month: Int,
  day: Int,
  site_id: Int,
)


class CountLogic(spark: SparkSession) {
  import spark.implicits._


  // cells table:
  //  year | month | day | cell_identity | frequency_band | site_id | technology
  // sites:
  //  year | month | day | site_id

  def techString(tech: String): String =
    tech match {
      case "gsm"  => "site_2g_cnt"
      case "umts" => "site_3g_cnt"
      case "lte"  => "site_4g_cnt"
    }

  def countTech(cells: DataFrame, sites: DataFrame): DataFrame = {

    sites.printSchema()
    sites.show()

    cells.printSchema()
    cells.show()

    val a = sites.join(cells, usingColumns = Seq("site_id"), joinType = "left")
    val b = a.groupBy("site_id", "technology").count()
    val c =
      b.groupBy("site_id").pivot("technology", List("gsm", "umts", "lte")).sum("count")
          .na.fill(0)
          .withColumnRenamed("gsm" , "site_2g_cnt")
          .withColumnRenamed("umts", "site_3g_cnt")
          .withColumnRenamed("lte" , "site_4g_cnt")

    c.show()
    c
  }

  def bandString(tech: String, band: Int) = {
    val t = tech match {
      case "gsm"  => "G"
      case "umts" => "U"
      case "lte"  => "L"
      case _      => "X" // TODO: decide to either Stop the world, or continue and mark error
    }
    s"frequency_band_$t$band"
  }

  def bandsPerSite(cells: DataFrame, sites: DataFrame): DataFrame = {
    println("CELLS")
    cells.show()

    println("SITES:")
    sites.show()

    println("Transform")

    val f: Column =
      translate('technology, "gsm", "frequency_band_G").as("extra")



    cells.withColumn("extra", f).show()

    val a = sites.join(cells, usingColumns = Seq("site_id"), joinType = "left")

    a.show()

    val b = a.groupBy("site_id", "technology").count()

    b.show()

    val c =
      b.groupBy("site_id").pivot("technology", List("gsm", "umts", "lte")).sum("count")
        .na.fill(0)
        .withColumnRenamed("gsm" , "site_2g_cnt")
        .withColumnRenamed("umts", "site_3g_cnt")
        .withColumnRenamed("lte" , "site_4g_cnt")

    c.show()
    c

  }
}
