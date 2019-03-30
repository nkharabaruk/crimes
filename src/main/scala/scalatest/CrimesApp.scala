package scalatest

import scalatest.processor.TheftsProcessor

/**
  * Application to analise and process most common crimes.
  */
object CrimesApp extends TheftsProcessor with App {

  /**
    * Dataset location in classpath resources.
    */
  val crimesFolder = "crimes"

  /**
    * Service to analise and work up crimes data.
    */
  val processor = new TheftsProcessor()

  processor.processThefts( crimesFolder )
}
