name := "TeleTest"
version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "2.4.4"

lazy val sparkJob =
  project.in(file("."))
    .settings(
      scalaVersion := "2.12.10",
      description := "SparkJob - count devices",
      mainClass in (Compile, run) := Some("com.telia.spark.CountDevices"),
      fork in Test := false,
      parallelExecution in Test := false,
      resolvers += Resolver.bintrayRepo("spark-packages", "maven"),   // for spark-fast-test lib
      libraryDependencies ++= Seq(

        "org.apache.spark" %% "spark-core"   % sparkVersion,
        "org.apache.spark" %% "spark-sql"    % sparkVersion,

        //"org.typelevel" %% "cats-core" % "2.0.0",
        //"io.univalence" %% "spark-test" % "0.3", // 2.12 depends on typedpath, typedpath only available for 2.11

        "org.mockito"       % "mockito-core"      % "2.7.22"        % "test" exclude("org.hamcrest", "hamcrest-core"),
        "org.scalatest"    %% "scalatest"         % "3.0.3"         % "test",
        "MrPowers"          % "spark-fast-tests"  % "0.20.0-s_2.12" % "test" // NOT FOUND
        //"io.univalence"     % "spark-test_2.12"  % "0.3"           % "test"
        //"io.univalence"    %% "spark-test"  % "0.3"           % "test"

      )

      // TODO: Remove if not dockerized
      // ,dockerBaseImage       := "openjdk:jre-alpine"
      // ,packageName in Docker := "michee/sparkJob"
      // ,version in Docker     := "1.0"
      // ,dockerExposedPorts    := Seq(8080, 8080)
    )
    //.enablePlugins(AshScriptPlugin)
    //.enablePlugins(DockerPlugin)