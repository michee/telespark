package com.telia.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/** Manages a local `ss` {@link SparkSession} variable, correctly stopping it after each test. */

trait LocalSparkSession extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>

  @transient var ss: SparkSession = _

  def sparkSession:SparkSession

  override def beforeEach() = {
    ss = sparkSession
    super.beforeEach()
  }

  override def beforeAll(): Unit = {
    // InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
    super.beforeAll()
  }

  override def afterEach(): Unit = {
    resetSparkSession()
    super.afterEach()
  }

  def resetSparkSession() = {
    LocalSparkSession.stop(ss)
    ss = null
  }

}

object LocalSparkSession {
  def stop(ss: SparkSession): Unit = {
    if (ss != null) {
      ss.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `ss` and ensures that `ss` is stopped. */
  def withSpark[T](ss: SparkSession)(f: SparkSession => T) = {
    try {
      f(ss)
    } finally {
      stop(ss)
    }
  }

}
