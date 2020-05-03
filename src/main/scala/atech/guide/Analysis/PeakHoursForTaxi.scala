package atech.guide.Analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object PeakHoursForTaxi {

  def apply(taxiDF: DataFrame, taxiZoneDF: DataFrame): Unit = {

    val pickUpByHourDF = taxiDF
      .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
      .groupBy("hour_of_day")
      .agg(count("*") as "TotalTrips")
      .orderBy(col("TotalTrips").desc_nulls_last)


    /**
      * +-----------+----------+
      * |hour_of_day|TotalTrips|
      * +-----------+----------+
      * |         22|     22108|
      * |         21|     20924|
      * |         23|     20903|
      * |          1|     20831|
      * |          0|     20421|
      * |         18|     18316|
      * |         11|     18270|
      * |         12|     17983|
      * |          2|     17097|
      * |         19|     16862|
      * |         17|     16741|
      * |         20|     16638|
      * |         15|     16194|
      * |         13|     15988|
      * |         16|     15613|
      * |         14|     15162|
      * |         10|     11964|
      * |          3|     10856|
      * |          9|      5358|
      * |          4|      5127|
      * +-----------+----------+
      *
      * We notice that we have clear peak hours with increased demand.
      *
      * Proposal -> Try to differentiate prices according to demand
      */
    pickUpByHourDF.show
  }

}
