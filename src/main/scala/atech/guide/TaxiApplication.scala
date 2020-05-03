package atech.guide

import atech.guide.Analysis.{MostPickupDropoffs, PeakHoursForLongShortTrips, PeakHoursForTaxi, TopPickUpAndDropOffForLongShortTrips, TripDistribution}
import org.apache.spark.sql.SparkSession

object TaxiApplication extends App {

  private val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val taxiDF = spark.read
    .load("src/main/resources/data/yellow_taxi_jan_25_2018")

  /**
    * root
    * |-- VendorID: integer (nullable = true)
    * |-- tpep_pickup_datetime: timestamp (nullable = true) [Pickup Timestamp]
    * |-- tpep_dropoff_datetime: timestamp (nullable = true) [drop off Timestamp]
    * |-- passenger_count: integer (nullable = true)
    * |-- trip_distance: double (nullable = true) [length of trip in miles]
    * |-- RatecodeID: integer (nullable = true) [ 1 (Standard), 2 (JFK), 3 (Newark), 4 (Nassau/Westchester), 5 (negotiated)]
    * |-- store_and_fwd_flag: string (nullable = true)
    * |-- PULocationID: integer (nullable = true) [Pickup location zone ID]
    * |-- DOLocationID: integer (nullable = true) [Drop off location zone ID]
    * |-- payment_type: integer (nullable = true) [ 1 (credit card), 2(cash), 3(no charge), 4 (dispute), 5 (unknown), 6 (voided)]
    * |-- fare_amount: double (nullable = true)
    * |-- extra: double (nullable = true)
    * |-- mta_tax: double (nullable = true)
    * |-- tip_amount: double (nullable = true)
    * |-- tolls_amount: double (nullable = true)
    * |-- improvement_surcharge: double (nullable = true)
    * |-- total_amount: double (nullable = true)
    */

  // taxiDF.printSchema
  // println(taxiDF.count) // 331893

  val taxiZoneDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  /**
    * root
    * |-- LocationID: integer (nullable = true)
    * |-- Borough: string (nullable = true)
    * |-- Zone: string (nullable = true)
    * |-- service_zone: string (nullable = true)
    */
   // taxiZoneDF.printSchema()

  // 1
  // MostPickupDropoffs(taxiDF, taxiZoneDF)

  // 2
  // PeakHoursForTaxi(taxiDF, taxiZoneDF)

  // 3
  // TripDistribution(taxiDF, taxiZoneDF)

  // 4
  // PeakHoursForLongShortTrips(taxiDF, taxiZoneDF)

  // 5
  TopPickUpAndDropOffForLongShortTrips(taxiDF, taxiZoneDF)


}
