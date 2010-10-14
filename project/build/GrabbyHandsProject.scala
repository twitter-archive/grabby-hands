import sbt._
import com.twitter.sbt._

class GrabbyHandsProject(info: ProjectInfo) extends StandardProject(info) with SubversionPublisher{
  val specs     = buildScalaVersion match {
    case "2.7.7" => "org.scala-tools.testing" % "specs" % "1.6.2.1"
    case _ => "org.scala-tools.testing" %% "specs" % "1.6.5"
  }
  val vscaladoc = "org.scala-tools" % "vscaladoc" % "1.1-md-3"
  val junit = "junit" % "junit" % "4.7"

  override def pomExtra =
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>

  override def releaseBuild = true
  override def disableCrossPaths = false

  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public")

  override def compileOptions = buildScalaVersion match {
    case "2.7.7" => {
      super.compileOptions ++ Seq(Unchecked) ++
      compileOptions("-encoding", "utf8") ++
      compileOptions("-deprecation")
    }
    case _ => {
      super.compileOptions ++ Seq(Unchecked) ++
      compileOptions("-encoding", "utf8") ++
      compileOptions("-deprecation") ++
      compileOptions("-Xmigration")
    }
  }

  override def testOptions = ExcludeTests("com.twitter.grabbyhands.SpecBase" :: "com.twitter.grabbyhands.BasicSpecStress" :: Nil) :: super.testOptions.toList
}
