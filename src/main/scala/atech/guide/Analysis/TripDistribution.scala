package atech.guide.Analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TripDistribution {

  def apply(taxiDF: DataFrame, taxiZoneDF: DataFrame): Unit = {

    val tripDistanceDF = taxiDF.select(col("trip_distance") as "distance")
    val longDistanceThreshold = 30

    val tripDistanceStatsDF = tripDistanceDF.select(
      count("*") as "count",
      lit(longDistanceThreshold) as "threshold",
      mean("distance") as "mean",
      stddev("distance") as "stddev",
      min("distance") as "min",
      max("distance") as "max"
    )

    /**
      * +------+---------+-----------------+-----------------+---+----+
      * | count|threshold|             mean|           stddev|min| max|
      * +------+---------+-----------------+-----------------+---+----+
      * |331893|       30|2.717989442380494|3.485152224885052|0.0|66.0|
      * +------+---------+-----------------+-----------------+---+----+
      */
    tripDistanceStatsDF.show

    val tripsWithLengthDF = taxiDF
      .withColumn("isLong", col("trip_distance") >= longDistanceThreshold)

    val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count

    /**
      * +------+------+
      * |isLong| count|
      * +------+------+
      * |  true|    83|
      * | false|331810|
      * +------+------+
      */
    tripsByLengthDF.show

  }

}
