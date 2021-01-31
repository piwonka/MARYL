import org.fusesource.scalate.TemplateEngine.log

import scala.sys.process.{Process, ProcessLogger}

name := "MARYL"
version := "0.1"
val scalaV = "2.13.4"
scalaVersion := scalaV
publishMavenStyle := true
//dependencies for Hadoop program
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-yarn-client" % "3.2.0",
  "org.apache.hadoop" % "hadoop-common" % "3.2.0",
  "org.scala-lang" % "scala-compiler" % scalaV,
  "org.scala-lang" % "scala-library" % scalaV,
  "org.scala-lang" % "scala-reflect" % scalaV,
)
//Compile/Build Options
scalacOptions += "-target:jvm-1.8"
javacOptions in(Compile, compile) ++= Seq("-source", "1.8", "-target", "1.8", "-g:lines")
mainClass in(Compile, run) := Some("main.Main") //specifying fully qualified path of main class
mainClass in(Compile, packageBin):=Some("main.Main")
mainClass in assembly:=Some("main.Main")// assembly=fatjar
assemblyJarName in assembly:="MARYL.jar"
crossPaths := false
autoScalaLibrary := false

