name := """100x.io"""

version := "1.0"

scalaVersion := "2.11.5"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.9",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.9" % "test",
  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test",
//  "org.scalatest" % "scalatest_2.11" % "3.0.0-SNAP4",
  "com.h2database" % "h2" % "1.4.185"
)

testOptions in Test += Tests.Argument("-oDF")