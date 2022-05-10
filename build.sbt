import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

organization := "com.spark.common"
name := "spark-common"

val sparkVersion = "2.4.8"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "org.apache.logging.log4j" % "log4j-api" % "2.12.1",
  "com.typesafe" % "config" % "1.3.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided",
  "org.scalacheck" %% "scalacheck" % "1.14.1" % "test"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}.jar"

updateOptions := updateOptions.value.withGigahorse(false)

coverageHighlighting := true
coverageMinimum := 95
coverageFailOnMinimum := true
coverageEnabled in Test := true

// We're going to use the release plugin please. Way better than maven!
// Note that we are skippins some steps as we are using the maven plugin. Should remove that later when we get publishArtifacts working
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,              // : ReleaseStep
  inquireVersions,                        // : ReleaseStep
//  runClean,                               // : ReleaseStep
//  runTest,                                // : ReleaseStep
  setReleaseVersion,                      // : ReleaseStep
  commitReleaseVersion,                   // : ReleaseStep, performs the initial git checks
  tagRelease,                             // : ReleaseStep
  publishArtifacts,                       // : ReleaseStep, checks whether `publishTo` is properly set up
  setNextVersion,                         // : ReleaseStep
  commitNextVersion,                      // : ReleaseStep
  pushChanges                             // : ReleaseStep, also checks that an upstream branch is properly configured
)

// Looper drops garbage into our git repo, so we need to ignore untracked files!
releaseIgnoreUntrackedFiles := true

//publishTo := {
//  val nexus = "https://repository.com/nexus/content/repositories/"
//  if (isSnapshot.value)
//    Some("snapshots" at nexus + "pangaea_snapshots")
//  else
//    Some("releases"  at nexus + "pangaea_releases")
//}

// This is a hack I stole from another project in a far away land (internet).
// Need to ask the looper admins to add this to the default sbt settings (https://www.scala-sbt.org/release/docs/Publishing.html#Credentials)
credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

parallelExecution in Test := false
