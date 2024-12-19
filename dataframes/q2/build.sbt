lazy val root = (project in file("."))
   .settings(
       name := "df2",
       version := "2.0",
       scalaVersion := "2.12.20"
   )

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.5.2" % "provided",
    "org.apache.spark" %% "spark-sql" % "3.5.2" % "provided"
)
