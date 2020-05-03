package atech.guide.Analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, sum}

/**
  *  Which zones have the most pickup / drop offs overall
  */
object MostPickupDropoffs {

  def apply(taxiDF: DataFrame, taxiZoneDF: DataFrame): Unit = {

    val pickupsByTaxiZoneDF = taxiDF.groupBy("PULocationID")
      .agg(count("*") as "TotalTrips")
      .join(taxiZoneDF, col("PULocationID") === col("LocationID"))
      .drop("LocationID", "service_zone")
      .orderBy(col("TotalTrips").desc_nulls_last)

    pickupsByTaxiZoneDF.show

    // 1b - group by Borough
    val pickupsByBorough = pickupsByTaxiZoneDF
      .groupBy("Borough")
      .agg(sum("TotalTrips") as "TotalTrips")
      .orderBy(col("TotalTrips").desc_nulls_last)


    /**
      * Single Day Analysis
      *
      * +-------------+----------+
      * |      Borough|TotalTrips|
      * +-------------+----------+
      * |    Manhattan|    304266|
      * |       Queens|     17712|
      * |      Unknown|      6644|
      * |     Brooklyn|      3037|
      * |        Bronx|       211|
      * |          EWR|        19|
      * |Staten Island|         4|
      * +-------------+----------+
      *
      * We see that data is extremely skewed towards Manhattan.
      *
      * Proposal: -> Differentiate prices according to the pickups/dropoffs area, and by demand
      *
      * We can slightly increase the prices for Manhattan as its popular
      * and slightly decrease the prices at other Borough as there by incentivize that other places in new york to use cab more
      */
    pickupsByBorough.show

  }
}
