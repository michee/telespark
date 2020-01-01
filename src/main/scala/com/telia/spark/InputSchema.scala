package com.telia.spark

import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

object InputSchema {
  // year;month;day;cell_identity;frequency_band;site_id
  // 2018;10;24;C;1800;3
  private val cellCSV = Seq(
    ("year"           , IntegerType),
    ("month"          , IntegerType),
    ("day"            , IntegerType),
    ("cell_identity"  , StringType),
    ("frequency_band" , IntegerType),
    ("site_id"        , IntegerType),
  )

  // year;month;day;site_id
  // 2018;10;24;3
  private val siteCSV = Seq(
    ("year"    , IntegerType),
    ("month"   , IntegerType),
    ("day"     , IntegerType),
    ("site_id" , IntegerType),
  )

  private def createSchema(t: Seq[(String, DataType)]): StructType =
    StructType(
      t.map{case (name, dType) => StructField(name, dType, nullable = false)}
    )

  val cellSchema = createSchema(cellCSV)
  val siteSchema = createSchema(siteCSV)
}
