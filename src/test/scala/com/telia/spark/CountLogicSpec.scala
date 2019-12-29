package com.telia.spark


import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import com.github.mrpowers.spark.fast.tests.{DataFrameComparer, DatasetContentMismatch}

import scala.collection.JavaConverters._

object Constants {
  val bands: Map[String, List[Int]] = Map(
    "gsm"  -> List(900, 1800),
    "umts" -> List(900, 2100),
    "lte"  -> List(700, 800, 1800, 2100, 2600)
  )
}

case class CountRow(
  site_id: Int,
  site_2g_cnt: Long,  // gsm
  site_3g_cnt: Long,  // umts
  site_4g_cnt: Long   // lte
)

class CountLogicSpec
  extends WordSpec
  with Matchers
  with LocalSparkSession
  with OneInstancePerTest
  with DataFrameComparer {

  val conf = new SparkConf(false).setMaster("local[4]")

  //overriding the sparkSession method from LocalSparkSession trait
  override def sparkSession =
    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

  val t = sparkSession
  import t.implicits._

  val countTypes = Seq(
    ("site_id"    , IntegerType),
    ("site_2g_cnt", LongType),
    ("site_3g_cnt", LongType),
    ("site_4g_cnt", LongType)
  )

  val countSchema = createSchema(countTypes)

  def createSchema(t: Seq[(String, DataType)]): StructType =
    StructType(
      t.map{case (name, dType) => StructField(name, dType, nullable = false)}
    )

  def toCountDF(c: Seq[CountRow]): DataFrame = {
    c.toDF("site_id", "site_2g_cnt", "site_3g_cnt", "site_4g_cnt")
  }

  def band(tech: String, band: Int): String = {
    val t = tech match {
      case "gsm"  => "G"
      case "umts" => "U"
      case "lte"  => "L"
    }
    s"frequency_band_$t$band"
  }

  def generateBandColumns(bandMap: Map[String, List[Int]]): Seq[String] = {
    bandMap.flatMap{
      case (k, bands) => bands.map(band(k, _))
    }.toSeq
  }

  def bandResultSchema(bandMap: Map[String, List[Int]]): StructType = {
    val bands = generateBandColumns(bandMap)
    StructType(
        StructField("site_id", IntegerType, false) +:
        bands.map(StructField(_, LongType, false))
    )
  }

  val bandResSchema = bandResultSchema(Constants.bands)

  val emptyRDD = sparkSession.sparkContext.emptyRDD[Row]

  def bandDTzero(): DataFrame = {
    val v   = 0 :: List.fill(bandResSchema.length - 1)(0: Long)
    val row = Row.fromSeq(v)
    ss.createDataFrame(List(row).asJava , bandResSchema)
  }


  "Comparing two DataFrames" should {
    "fail for different lenght" in {
      val a = Seq[Int](1).toDF("a")
      val b = Seq[Int](1, 1).toDF("a")

      assertThrows[DatasetContentMismatch] {
        assertSmallDataFrameEquality(a, b)
      }
    }
  }

  "CellsPerTech" should {

    "Handle unknown technology" in {
      pending
    }

    "Disregard data entries with the wrong day" in {
      pending
    }

    "Handle empty Sites" in {
      val cell = Seq(
        CellRow(2019, 10, 1, "A", 900, 1, "gsm")
      ).toDF
      val site   = List.empty[SiteRow].toDF
      val result = new CountLogic(ss).countTech(cell, site).coalesce(1)
      val expect = List.empty[CountRow].toDF
      assertSmallDataFrameEquality(result, expect, ignoreNullable = true)
    }

    "Handle empty Cells" in {
      val cell   = List.empty[CellRow].toDF
      val site   = Seq(SiteRow(2019, 10, 1, 1)).toDF
      val result = new CountLogic(ss).countTech(cell, site).coalesce(1)
      val expect = Seq(CountRow(1, 0, 0, 0)).toDF
      assertSmallDataFrameEquality(result, expect, ignoreNullable = true)
    }

    "Handle empty Cells and empty Sites" in {
      val cell   = List.empty[CellRow].toDF
      val site   = List.empty[SiteRow].toDF
      val result = new CountLogic(ss).countTech(cell, site).coalesce(1)
      val expect = List.empty[CountRow].toDF
      assertSmallDataFrameEquality(result, expect, ignoreNullable = true)
    }

    "Drop cells that don't have a site" in {
      val siteA = 1
      val siteB = 2

      val cell = Seq(
        CellRow(2019, 10, 1, "A", 900, siteA, "gsm"),
        CellRow(2019, 10, 1, "B", 900, siteB, "gsm")
      ).toDF

      val site = Seq(
        SiteRow(2019, 10, 1, siteA)
      ).toDF

      val result = new CountLogic(ss).countTech(cell, site).coalesce(1)

      val expect = Seq(CountRow(siteA, 1, 0, 0)).toDF
      assertSmallDataFrameEquality(result, expect, ignoreNullable = true)
    }

    "Count the Technology correctly" in {
      val siteA = 1

      val cell = Seq(
        CellRow(2019, 10, 1, "A", 900, siteA, "lte"),
        CellRow(2019, 10, 1, "B", 900, siteA, "umts"),
        CellRow(2019, 10, 1, "C", 900, siteA, "gsm"),
        CellRow(2019, 10, 1, "D", 900, siteA, "lte"),
        CellRow(2019, 10, 1, "E", 900, siteA, "umts"),
        CellRow(2019, 10, 1, "F", 900, siteA, "lte")
      ).toDF

      val site = Seq(
        SiteRow(2019, 10, 1, siteA)
      ).toDF

      val result = new CountLogic(ss).countTech(cell, site).coalesce(1)

      val expect = Seq(CountRow(siteA, 1, 2, 3)).toDF
      assertSmallDataFrameEquality(result, expect, ignoreNullable = true)
    }

  }

  "Generate Band string" should {
    "Generate the correct strings" in {
      band("gsm", 100)   shouldBe "frequency_band_G100"
      band("lte", 400)   shouldBe "frequency_band_L400"
      band("umts", 5200) shouldBe "frequency_band_U5200"
    }
  }

  "BandsPerSite" should {

    "Disregard data entries with the wrong day" in {
      pending
    }

    "Handle empty Sites" in {
      val cell = Seq(
        CellRow(2019, 10, 1, "A", 900, 1, "gsm")
      ).toDF
      val site   = List.empty[SiteRow].toDF
      val result = new CountLogic(ss).bandsPerSite(cell, site).coalesce(1)

      val expect = ss.createDataFrame(emptyRDD, bandResSchema)
      assertSmallDataFrameEquality(result, expect, ignoreNullable = true)
    }

    "Handle empty Cells" in {
      val siteA  = 1

      val cell   = List.empty[CellRow].toDF
      val site   = Seq(SiteRow(2019, 10, 1, siteA)).toDF

      val result = new CountLogic(ss).bandsPerSite(cell, site).coalesce(1)

      val expected = bandDTzero()
          .withColumn("site_id", lit(siteA))

      assertSmallDataFrameEquality(result, expected, ignoreNullable = true)
    }

    "Handle empty Cells and empty Sites" in {
      val cell   = List.empty[CellRow].toDF
      val site   = List.empty[SiteRow].toDF
      val result = new CountLogic(ss).bandsPerSite(cell, site).coalesce(1)
      val expect = List.empty[CountRow].toDF
      assertSmallDataFrameEquality(result, expect, ignoreNullable = true)
    }

    "Drop cells that don't have a site" in {
      val siteA = 1
      val siteB = 2

      val cell = Seq(
        CellRow(2019, 10, 1, "A", 900, siteA, "gsm"),
        CellRow(2019, 10, 1, "B", 900, siteB, "gsm")
      ).toDF

      val site = Seq(
        SiteRow(2019, 10, 1, siteA)
      ).toDF

      val result = new CountLogic(ss).bandsPerSite(cell, site).coalesce(1)

      val expected = bandDTzero()
          .withColumn("site_id"            , lit(siteA))
          .withColumn("frequency_band_G900", lit(1))

      assertSmallDataFrameEquality(result, expected, ignoreNullable = true)
    }

    "Count the Technology correctly" in {
      val siteA = 1

      val cell = Seq(
        CellRow(2019, 10, 1, "A", 700, siteA, "lte"),
        CellRow(2019, 10, 1, "B", 900, siteA, "umts"),
        CellRow(2019, 10, 1, "D", 700, siteA, "lte")
      ).toDF

      val site = Seq(
        SiteRow(2019, 10, 1, siteA)
      ).toDF

      val result = new CountLogic(ss).bandsPerSite(cell, site).coalesce(1)

      val expect = bandDTzero()
          .withColumn("site_id", lit(siteA))
          .withColumn("frequency_band_L700", lit(2))
          .withColumn("frequency_band_U900", lit(1))

      assertSmallDataFrameEquality(result, expect, ignoreNullable = true)
    }

    "Count frequency bands correctly" in {
      val siteA = 1

      // LTT Bands 700, 800, 1800, 2100, 2600
      val cell = Seq(
        CellRow(2019, 10, 1, "A",  700, siteA, "lte"),
        CellRow(2019, 10, 1, "B",  700, siteA, "lte"),
        CellRow(2019, 10, 1, "C", 2100, siteA, "lte"),
        CellRow(2019, 10, 1, "D", 2600, siteA, "lte"),
        CellRow(2019, 10, 1, "E", 2100, siteA, "lte"),
      ).toDF

      val site = Seq(
        SiteRow(2019, 10, 1, siteA)
      ).toDF

      val result = new CountLogic(ss).bandsPerSite(cell, site).coalesce(1)

      val expect = bandDTzero()
          .withColumn("site_id", lit(siteA))
          .withColumn("frequency_band_L700",  lit(2))
          .withColumn("frequency_band_L2100", lit(2))
          .withColumn("frequency_band_L2600", lit(1))

      assertSmallDataFrameEquality(result, expect, ignoreNullable = true)
    }

    "disregard unknown technology" in {
      val siteA = 1

      val cell = Seq(
        CellRow(2019, 10, 1, "A", 700, siteA, "wierdo"),
        CellRow(2019, 10, 1, "B", 700, siteA,    "lte")
      ).toDF

      val site = Seq(
        SiteRow(2019, 10, 1, siteA)
      ).toDF

      val result = new CountLogic(ss).bandsPerSite(cell, site).coalesce(1)

      val expect = bandDTzero()
        .withColumn("site_id", lit(siteA))
        .withColumn("frequency_band_L700", lit(1))

      assertSmallDataFrameEquality(result, expect, ignoreNullable = true)
    }

    "disregard unknown band" in {
      val siteA = 1

      val cell = Seq(
        CellRow(2019, 10, 1, "A",    700, siteA, "lte"),
        CellRow(2019, 10, 1, "B", 999999, siteA, "lte")
      ).toDF

      val site = Seq(
        SiteRow(2019, 10, 1, siteA)
      ).toDF

      val result = new CountLogic(ss).bandsPerSite(cell, site).coalesce(1)

      val expect = bandDTzero()
        .withColumn("site_id", lit(siteA))
        .withColumn("frequency_band_L700",  lit(1))

      assertSmallDataFrameEquality(result, expect, ignoreNullable = true)
    }
  }
}

