
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
      val path = "src/test/resources/" + testFile // CountDevices.fsPath(testFile)
      val schema = CountDevices.cellSchema

      val dt = CountDevices.read(ss, path, schema)
      dt.show()

      dt.count() shouldBe 2
    }

    "create dataframe with correct types from Site csv" in {
      val testFile = "site_test.csv"
      val path = "src/test/resources/" + testFile // CountDevices.fsPath(testFile)
      val schema = CountDevices.siteSchema

      val dt = CountDevices.read(ss, path, schema)

      dt.show()

      dt.printSchema()

      dt.count() shouldBe 1
    }
  }

  "Archive client" should {
    "list all days in archive" in {
      val archive = new ArchiveClient(ss, "src/test/resources/testArchive")
      val result = archive.getAllDaysFromDir("technology_A")

      val expected =
        Set(
          Day(2019, 10, 2),
          Day(2019, 11, 13),
          Day(2019, 11, 14)
        )

      result shouldBe expected
    }

    "list days that are missing a result" in {
      val archive = new ArchiveClient(ss, "src/test/resources/testArchive")
      val result  = archive.getMissingDays(List("technology_A"), "result")

      val expected =
        Set(
          Day(2019, 10, 2),
          Day(2019, 11, 14)
        )

      result shouldBe expected
    }

    "crate url from Day" in {
      val archive = new ArchiveClient(ss, "src/test/resources/testArchive")

      archive.toUrl(Day(2019, 11, 23)) shouldBe "/year=2019/month=11/day=23/"
      archive.toUrl(Day(2018,  1,  1)) shouldBe "/year=2018/month=1/day=1/"
    }


    "return empty list if a specific day dont exist" in {
      val archive = new ArchiveClient(ss, "src/test/resources/testArchive")
      val a = archive.getSingleCSVFile("technology_A", Day(2019, 11, 2))
      a shouldBe List.empty
    }

    "get CVS files from a specific dir for a specific day" in {
      val archive = new ArchiveClient(ss, "src/test/resources/testArchive")

      val A = archive.getSingleCSVFile("technology_A", Day(2019, 11, 13))
      A.size shouldBe 1
      A.head.getName shouldBe "1.csv"

      val B = archive.getSingleCSVFile("technology_A", Day(2019, 11, 14))
      B.size shouldBe 1
      B.head.getName shouldBe "2.csv"

      val C = archive.getSingleCSVFile("result", Day(2019, 11, 13))
      C.size shouldBe 1
      C.head.getName shouldBe "result.csv"
    }

  }
}

