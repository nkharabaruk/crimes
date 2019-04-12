package com.scalatest.processor

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
import scala.collection.mutable

/**
  * Integration test for the Thefts Processor.
  */
class TheftsProcessorSpec extends FlatSpec with Matchers {

  /**
    * Dataset location in test classpath resources.
    * Used smaller dataset for test purposes.
    */
  val crimesFolder = "crimes_test"

  "The TheftsProcessor" should "retrieve most common crimes locations" in {
    val sut = new TheftsProcessor()
    val result = sut.processThefts( crimesFolder  )
    verifyResult( result, sut.theftsNumber )
  }

  /**
    * Verifies common specification result dataset should responds.
    * @param result the result data array to check.
    */
  private def verifyResult( result : Array[Row], theftsNumber : Int ): Unit = {
    result.length shouldEqual theftsNumber
    result.foreach( row => {
      def longitude = row.getAs[String]( 0 )
      longitude != null && !longitude.isEmpty shouldBe true
      def latitude = row.getAs[String]( 1 )
      latitude != null && !latitude.isEmpty shouldBe true
      def crimeTypes = row.getAs[mutable.WrappedArray[String]]( 2 )
      crimeTypes.nonEmpty shouldBe true
      def crimesCount = row.getAs[Long]( 3 )
      crimesCount > 0 shouldBe true
    } )
    val crimesNumber = result.map( row => row.getAs[Long]( 3 ) )
    crimesNumber.sorted( Ordering.Long.reverse ) shouldEqual crimesNumber // data is sorted
  }
}
