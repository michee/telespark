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

  /**
   * Technology and Frequencies
   */
  val bands: Map[String, List[Int]] = Map(
    "gsm"  -> List(900, 1800),
    "umts" -> List(900, 2100),
    "lte"  -> List(700, 800, 1800, 2100, 2600)
  )

  // cells table:
  //  year | month | day | cell_identity | frequency_band | site_id | technology
  // sites:
  //  year | month | day | site_id

  def countTech(cells: DataFrame, sites: DataFrame): DataFrame = {

    val a = sites.join(cells, usingColumns = Seq("site_id"), joinType = "left")
    val b = a.groupBy("site_id", "technology").count()
    val c =
      b.groupBy("site_id").pivot("technology", List("gsm", "umts", "lte")).sum("count")
          .na.fill(0)
          .withColumnRenamed("gsm" , "site_2g_cnt")
          .withColumnRenamed("umts", "site_3g_cnt")
          .withColumnRenamed("lte" , "site_4g_cnt")
    c
  }


  /** UDF, not used in favor of DataFrame API's
    */
  def bandString(tech: String, band: Int) = {
    val t = tech match {
      case "gsm"  => "G"
      case "umts" => "U"
      case "lte"  => "L"
      case _      => "_"
    }
    s"frequency_band_$t$band"
  }

  def listTechBands(bandMap: Map[String, List[Int]]): Seq[String] = {
    bandMap.flatMap{
      case (k, bands) => bands.map(bandString(k, _))
    }.toSeq
  }

  val allTechnologyBandCombos: Seq[String] = listTechBands(bands)

  /**
   * Note:
   * Since the Task is to base a solution on DataFrames
   * I avoided using UDF's and tried to express transformation functions using the DataFrame API
   * as much as possible.
   *
   */
  def bandsPerSite(cells: DataFrame, sites: DataFrame): DataFrame = {

    def renameTech(c: Column): Column =
       when(c === lit("gsm"),  lit("frequency_band_G"))
      .when(c === lit("umts"), lit("frequency_band_U"))
      .when(c === lit("lte"),  lit("frequency_band_L"))

    val renamedCells =
      cells
        .withColumn("technology", renameTech('technology))
        .withColumn("technology", concat('technology, lit(""), 'frequency_band))

    val a = sites.join(renamedCells, usingColumns = Seq("site_id"), joinType = "left")
    val b = a.groupBy("site_id", "technology").count()
    val c =
      b.groupBy("site_id").pivot("technology", allTechnologyBandCombos).sum("count")
        .na.fill(0)
    c
  }

  def applyDate(df: DataFrame, day: Day): DataFrame = {
    df
      .withColumn("year",  lit(day.year))
      .withColumn("month", lit(day.month))
      .withColumn("day",   lit(day.day))
  }
}
