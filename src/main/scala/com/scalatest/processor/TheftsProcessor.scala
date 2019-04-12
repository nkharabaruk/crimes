package com.scalatest.processor

import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{collect_set, count, desc}
import scala.collection.mutable

/**
  * Service to analise and work up crimes data.
  */
class TheftsProcessor extends Serializable {

  /**
    * Name of the application.
    */
  val appName = "crimes"

  /**
    * Number of thefts result dataset should contain.
    */
  val theftsNumber = 5

  /**
    * Works up data about the thefts from the given folder.
    * @param filesFolder the folder where the files are located in.
    * @return array with processed thefts rows.
    */
  def processThefts( filesFolder: String ): Array[Row] = {

    val rows = readData( filesFolder )
      .filter( row => isNotEmpty( row, "Crime ID" ) )
      .groupBy( "Longitude", "Latitude" )
      .agg( collect_set( "Crime type" ).as( "Crimes types" ), count( "*" ).as( "Crimes count" ) )
      .filter( row => isNotEmpty( row, "Longitude" )  && isNotEmpty( row, "Latitude" ) )
      .sort( desc( "Crimes count" ) )
      .limit( theftsNumber )

    printThefts( rows )

    rows.collect
  }

  /**
    * Setups spark context and retrieves csv data from given folder.
    * @param filesFolder the folder data files are located in.
    * @return the data frame object with retrieved data rows.
    */
  private def readData( filesFolder: String ): DataFrame = {
    val filePaths = new File( getClass.getClassLoader.getResource( filesFolder ).getFile ).listFiles.toList.map( file => file.getPath )
    val config = new SparkConf().setAppName( appName ).setMaster( "local[4]" )
    val spark = SparkSession.builder.config( config ).getOrCreate
    spark.read.option( "header", "true" ).csv( filePaths: _* )
  }

  /**
    * Prints out to the console formatted processed data about the thefts.
    * @param rows the data set with thefts information.
    */
  private def printThefts( rows: Dataset[Row] ): Unit = {
    println( "-------------------------------------" )
    rows.foreach( row => {
      println( "(" + row.getAs[String]( "Longitude" ) + "," + row.getAs[String]( "Latitude" ) + ")" + ": " + row.getAs[String]( "Crimes count" ) )
      println( "Thefts:" )
      row.getAs[mutable.WrappedArray[String]]( "Crimes types" )
        .foreach ( innerRow => println( innerRow ) )
      println("-------------------------------------")
    } )
  }

  /**
    * Checks of the given row is not empty.
    * @param row the row to check.
    * @param rowName the name of the row.
    * @return true if row has some data and false otherwise.
    */
  private def isNotEmpty( row: Row, rowName: String ): Boolean = {
    val column = row.getAs[String]( rowName )
    column != null && !column.isEmpty
  }
}
