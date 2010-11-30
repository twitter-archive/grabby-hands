import sbt._
import com.twitter.sbt._

class GrabbyHandsProject(info: ProjectInfo) extends StandardProject(info) with SubversionPublisher{
  val specs     = buildScalaVersion match {
    case "2.7.7" => "org.scala-tools.testing" % "specs" % "1.6.2.1" % "test"
    case _ => "org.scala-tools.testing" %% "specs" % "1.6.5" % "test"
  }
  val junitInterface = "com.novocode" % "junit-interface" % "0.5" % "test->default"

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
      compileOptions("-deprecation")
      // ++ compileOptions("-Xmigration")
    }
  }

  def testFilter(include: Boolean, testRegex: String)(testName: String) = {
    val nameMatch = testRegex.r.findFirstIn(testName) match {
      case Some(s) => true
      case _ => false
    }
    if (nameMatch) {
      include
    } else {
      !include
    }
  }

  // test should NOT include stress tests
  override def testAction = defaultTestTask(TestFilter(testFilter(false, "Stress$|Base$")) :: testOptions.toList)
  // stress should ONLY include stress tests
  lazy val stress = defaultTestTask(TestFilter(testFilter(true, "Stress$")) :: testOptions.toList) describedAs "run stress tests"
}
