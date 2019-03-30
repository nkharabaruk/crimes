package scalatest

import org.scalatest._

/**
  * Main test of the crimes processing application.
  */
class CrimesAppSpec extends FlatSpec with Matchers {

  /**
    * Dataset location in test classpath resources.
    * Used smaller dataset for test purposes.
    */
  val crimesFolder = "crimes_test"

  "The CrimesApp" should "run and print result to console" in {
    CrimesApp.processor.processThefts( crimesFolder )
  }
}
