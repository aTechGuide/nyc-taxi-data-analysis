package atech.guide.Analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object PeakHoursForLongShortTrips {

  def apply(taxiDF: DataFrame, taxiZoneDF: DataFrame): Unit = {

    val longDistanceThreshold = 30
    val tripsWithLengthDF = taxiDF
      .withColumn("isLong", col("trip_distance") >= longDistanceThreshold)

    val pickUpByHourByLengthDF = tripsWithLengthDF
      .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
      .groupBy("hour_of_day", "isLong")
      .agg(count("*") as "TotalTrips")
      .orderBy(col("TotalTrips").desc_nulls_last)

    /**
      * +-----------+------+----------+
      * |hour_of_day|isLong|TotalTrips|
      * +-----------+------+----------+
      * |22         |false |22104     |
      * |21         |false |20921     |
      * |23         |false |20897     |
      * |1          |false |20824     |
      * |0          |false |20412     |
      * |18         |false |18315     |
      * |11         |false |18264     |
      * |12         |false |17980     |
      * |2          |false |17094     |
      * |19         |false |16858     |
      * |17         |false |16734     |
      * |20         |false |16635     |
      * |15         |false |16190     |
      * |13         |false |15985     |
      * |16         |false |15610     |
      * |14         |false |15158     |
      * |10         |false |11961     |
      * |3          |false |10853     |
      * |9          |false |5358      |
      * |4          |false |5124      |
      * |5          |false |3194      |
      * |6          |false |1971      |
      * |8          |false |1803      |
      * |7          |false |1565      |
      * |0          |true  |9         |
      * |17         |true  |7         |
      * |1          |true  |7         |
      * |23         |true  |6         |
      * |11         |true  |6         |
      * |15         |true  |4         |
      * |19         |true  |4         |
      * |14         |true  |4         |
      * |22         |true  |4         |
      * |3          |true  |3         |
      * |16         |true  |3         |
      * |12         |true  |3         |
      * |20         |true  |3         |
      * |10         |true  |3         |
      * |2          |true  |3         |
      * |4          |true  |3         |
      * |21         |true  |3         |
      * |13         |true  |3         |
      * |5          |true  |2         |
      * |6          |true  |2         |
      * |18         |true  |1         |
      * +-----------+------+----------+
      *
      * As long trips are very less, long trips data is lagging behind short trips data by 2 or 3 orders of magnitude.
      * So this data is really not relevant
      *s
      */
    pickUpByHourByLengthDF.show(48, truncate = false) // 24 for short and 24 for long trips

  }

}
