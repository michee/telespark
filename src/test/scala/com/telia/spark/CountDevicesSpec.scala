
package com.telia.spark


import com.telia.spark.CountDevices.conf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

import scala.io.Source

class CountDevicesSpec extends WordSpec with Matchers with LocalSparkSession with OneInstancePerTest {

  val conf = new SparkConf(false).setMaster("local[4]")

  //overriding the sparkSession method from LocalSparkSession trait
  override def sparkSession =
    SparkSession
    .builder()
    .config(conf)
    .getOrCreate() // new SparkContext("local", "test", conf)


  "CountDevices" should {

    "create dataframe with correct types from Cell/Technology csv" in {
      val testFile = "cell_test.csv"
      val path     = "src/test/resources/" + testFile // CountDevices.fsPath(testFile)
      val schema   = CountDevices.cellSchema

      val dt = CountDevices.read(ss, path, schema)
      dt.show()

      dt.count() shouldBe 2
    }

    "create dataframe with correct types from Site csv" in {
      val testFile = "site_test.csv"
      val path     = "src/test/resources/" + testFile // CountDevices.fsPath(testFile)
      val schema   = CountDevices.siteSchema

      val dt = CountDevices.read(ss, path, schema)
      dt.show()

      dt.count() shouldBe 1
    }
  }
}

