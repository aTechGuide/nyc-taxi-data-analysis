package atech.guide.Analysis

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object TopPickUpAndDropOffForLongShortTrips {

  def apply(taxiDF: DataFrame, taxiZoneDF: DataFrame): Unit = {

    val longDistanceThreshold = 30
    val tripsWithLengthDF = taxiDF
      .withColumn("isLong", col("trip_distance") >= longDistanceThreshold)

    def pickUpDropOffByPopularity(predicate: Column) = tripsWithLengthDF
        .where(predicate)
        .groupBy(col("PULocationID"), col("DOLocationID"))
        .agg(count("*") as "TotalTrips")
        .join(taxiZoneDF, col("PULocationID") === col("LocationID"))
        .withColumnRenamed("Zone", "Pickup_zone")
        .drop("LocationID", "Borough", "service_zone")
        .join(taxiZoneDF, col("DOLocationID") === col("LocationID"))
        .withColumnRenamed("Zone", "Dropoff_zone")
        .drop("LocationID", "Borough", "service_zone")
        .drop("PULocationID", "DOLocationID")
        .orderBy(col("TotalTrips").desc_nulls_last)


    /**
      * Single Day Analysis
      *
      * +----------+--------------------+--------------------+
      * |TotalTrips|         Pickup_zone|        Dropoff_zone|
      * +----------+--------------------+--------------------+
      * |        14|         JFK Airport|                  NA|
      * |         8|   LaGuardia Airport|                  NA|
      * |         4|         JFK Airport|         JFK Airport|
      * |         4|         JFK Airport|      Newark Airport|
      * |         3|       Midtown South|                  NA|
      * |         3|                  NV|                  NV|
      * |         2|   LaGuardia Airport|      Newark Airport|
      * |         2|        Clinton East|                  NA|
      *
      * Mostly Airport transfers for long trips
      */
    pickUpDropOffByPopularity(col("isLong")).show // <- Long Trips Only


    /**
      * Single Day Analysis
      *
      * +----------+--------------------+--------------------+
      * |TotalTrips|         Pickup_zone|        Dropoff_zone|
      * +----------+--------------------+--------------------+
      * |      5558|                  NV|                  NV|
      * |      2425|Upper East Side S...|Upper East Side N...|
      * |      1962|Upper East Side N...|Upper East Side S...|
      * |      1944|Upper East Side N...|Upper East Side N...|
      * |      1928|Upper East Side S...|Upper East Side S...|
      * |      1052|Upper East Side S...|      Midtown Center|
      * |      1012|Upper East Side S...|        Midtown East|
      * |       987|      Midtown Center|Upper East Side S...|
      * |       965|Upper West Side S...|Upper West Side N...|
      * |       882|      Midtown Center|      Midtown Center|
      *
      * Mostly High Class zones for short trips
      * Short trips are in between wealthy zones (bars, restaurants)
      */
    pickUpDropOffByPopularity(not(col("isLong"))).show // <- Short Trips Only


    /**
      * We can suggest to NYC town hall: To create rapid transit
      *
      * To Taxi Company: Separate market segment and tailor services to each. Also strike a partnership with bats/ restaurants for pick up service.
      */

  }

}
