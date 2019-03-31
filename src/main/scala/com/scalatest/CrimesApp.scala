package com.scalatest

import com.scalatest.processor.TheftsProcessor

/**
  * Application to analise and process most common crimes.
  */
object CrimesApp extends App {

  /**
    * Main entry point to the program.
    * @param args array of arguments to pass, should contain one element.
    */
  override def main( args: Array[String] ): Unit = {
    val crimesFolder = retrieveCrimesFolder( args )
    val processor = new TheftsProcessor()
    processor.processThefts( crimesFolder )
  }

  /**
    * Retrieved name of the folder where to retrieve data for crimes.
    * If the argument is signed in input parameters then use it, otherwise use default folder.
    * @param args array of arguments with name of crimes data path.
    * @return name of the folder where to retrieve data.
    */
  private def retrieveCrimesFolder( args: Array[String] ): String = {
    var crimesFolder = "crimes" // default value
    if ( args != null && !args.isEmpty ) {
      crimesFolder = args( 0 )
    }
    crimesFolder
  }
}
