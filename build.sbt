name := "TeleTest"
version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "2.4.4"

lazy val sparkJob =
  project.in(file("."))
    .settings(
      description := "SparkJob - count devices",
      mainClass in (Compile, run) := Some("com.telia.spark.CountDevices"),
      libraryDependencies ++= Seq(

        "org.apache.spark"  %% "spark-core"   % sparkVersion,
        "org.apache.spark"  %% "spark-sql"    % sparkVersion,

        "org.mockito"        % "mockito-core" % "2.7.22"   % "test" exclude("org.hamcrest", "hamcrest-core"),
        "org.scalatest"     %% "scalatest"    % "3.0.3"    % "test"
      )

      // TODO: Remove if not dockerized
      // ,dockerBaseImage       := "openjdk:jre-alpine"
      // ,packageName in Docker := "michee/sparkJob"
      // ,version in Docker     := "1.0"
      // ,dockerExposedPorts    := Seq(8080, 8080)
    )
    //.enablePlugins(AshScriptPlugin)
    //.enablePlugins(DockerPlugin)