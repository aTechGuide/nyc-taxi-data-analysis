package atech.guide.Analysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object RideSharingOppertunity {

  def apply(taxiDF: DataFrame, taxiZoneDF: DataFrame)(implicit spark: SparkSession): Unit = {

    // All the trips that can be grouped by location ID and 5 mins bucket.
    val groupAttemptsDF = taxiDF
      .select(
        round(unix_timestamp(col("tpep_pickup_datetime")) / 300 ).cast("integer") as "FiveMinId",
        col("PULocationID"),
        col("total_amount")
      )
      .where(col("passenger_count") < 3) // <- Filtering on atmost two passengers
      .groupBy(col("FiveMinId"), col("PULocationID")) // <- Grouping the trip starting from same location within 5 min time window
      .agg(count("*") as "TotalTrips", sum(col("total_amount")) as "TotalAmount")
      .orderBy(col("TotalTrips").desc_nulls_last)
      .withColumn("Approx_Datetime", from_unixtime(col("FiveMinId") * 300))
      .drop("FiveMinId")
      .join(taxiZoneDF, col("PULocationID") === col("LocationID"))
      .drop("LocationID", "service_zone")


    /**
      * Single Day Analysis
      *
      * +------------+----------+------------------+-------------------+---------+--------------------+
      * |PULocationID|TotalTrips|       TotalAmount|    Approx_Datetime|  Borough|                Zone|
      * +------------+----------+------------------+-------------------+---------+--------------------+
      * |         237|       115| 1376.199999999999|2018-01-25 18:45:00|Manhattan|Upper East Side S...|
      * |         236|       110|1308.1399999999985|2018-01-25 11:35:00|Manhattan|Upper East Side N...|
      * |         236|       105|1128.3999999999992|2018-01-25 18:35:00|Manhattan|Upper East Side N...|
      * |         237|       104|1164.9699999999991|2018-01-25 18:10:00|Manhattan|Upper East Side S...|
      * |         142|       103|1393.9899999999984|2018-01-26 01:40:00|Manhattan| Lincoln Square East|
      * |         142|       102|1410.8599999999985|2018-01-26 01:35:00|Manhattan| Lincoln Square East|
      * |         236|       101|1087.0899999999988|2018-01-25 18:30:00|Manhattan|Upper East Side N...|
      * |         237|       100|1215.0499999999988|2018-01-25 21:55:00|Manhattan|Upper East Side S...|
      * |         142|        99|1372.2099999999987|2018-01-26 01:05:00|Manhattan| Lincoln Square East|
      * |         162|        99|1615.1199999999983|2018-01-25 22:35:00|Manhattan|        Midtown East|
      * |         237|        99|1224.8099999999993|2018-01-25 23:10:00|Manhattan|Upper East Side S...|
      * |         161|        97| 1352.659999999999|2018-01-26 00:05:00|Manhattan|      Midtown Center|
      *
      * Within 5 min time window we have ~100 trips starting at same location ID. i.e. lots of close taxi rides
      * Which is an opportunity to do ride sharing for taxis
      *
      * - We can incentivize people to take a grouped ride, at a discount
      * - We produce fewer emissions - This project has significant potential for environment improvement,
      *   we can investigate the possibility of a subsidy from the government
      */
    // groupAttemptsDF.show


    //

    import spark.implicits._

    val percentGroupAttempt = 0.05
    val percentAcceptGrouping = 0.3
    val discount = 5
    val extraCost = 2
    val averageCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

    val groupingExtimatedEconomicDF = groupAttemptsDF
      .withColumn("groupedRides", col("TotalTrips") * percentGroupAttempt)
      .withColumn("acceptedGroupedRidesEconomucImpact", col("groupedRides") * percentAcceptGrouping * (averageCostReduction - discount))
      .withColumn("rejectedGroupedRidesEconomicRides", col("groupedRides") * (1 - percentGroupAttempt) * extraCost)
      .withColumn("totalImpact", col("acceptedGroupedRidesEconomucImpact") + col("rejectedGroupedRidesEconomicRides"))


    /**
      * Single Day Analysis
      *
      * +------------+----------+------------------+-------------------+---------+----------------------------+------------------+----------------------------------+---------------------------------+------------------+
      * |PULocationID|TotalTrips|TotalAmount       |Approx_Datetime    |Borough  |Zone                        |groupedRides      |acceptedGroupedRidesEconomucImpact|rejectedGroupedRidesEconomicRides|totalImpact       |
      * +------------+----------+------------------+-------------------+---------+----------------------------+------------------+----------------------------------+---------------------------------+------------------+
      * |237         |115       |1376.199999999999 |2018-01-25 18:45:00|Manhattan|Upper East Side South       |5.75              |7.827847922779476                 |10.924999999999999               |18.752847922779473|
      * |236         |110       |1308.1399999999985|2018-01-25 11:35:00|Manhattan|Upper East Side North       |5.5               |7.487506708745586                 |10.45                            |17.937506708745588|
      * |236         |105       |1128.3999999999992|2018-01-25 18:35:00|Manhattan|Upper East Side North       |5.25              |7.147165494711696                 |9.975                            |17.122165494711695|
      * |237         |104       |1164.9699999999991|2018-01-25 18:10:00|Manhattan|Upper East Side South       |5.2               |7.079097251904918                 |9.879999999999999                |16.959097251904918|
      * |142         |103       |1393.9899999999984|2018-01-26 01:40:00|Manhattan|Lincoln Square East         |5.15              |7.011029009098141                 |9.785                            |16.79602900909814 |
      * |142         |102       |1410.8599999999985|2018-01-26 01:35:00|Manhattan|Lincoln Square East         |5.1000000000000005|6.942960766291362                 |9.690000000000001                |16.63296076629136 |
      * |236         |101       |1087.0899999999988|2018-01-25 18:30:00|Manhattan|Upper East Side North       |5.050000000000001 |6.874892523484585                 |9.595                            |16.469892523484585|
      * |237         |100       |1215.0499999999988|2018-01-25 21:55:00|Manhattan|Upper East Side South       |5.0               |6.806824280677806                 |9.5                              |16.306824280677805|
      * |142         |99        |1372.2099999999987|2018-01-26 01:05:00|Manhattan|Lincoln Square East         |4.95              |6.7387560378710285                |9.405                            |16.14375603787103 |
      *
      */
    groupingExtimatedEconomicDF.show(100, truncate = false)

    val totalProfit = groupingExtimatedEconomicDF.select(sum(col("totalImpact")) as "Total")

    /**
      * Single Day Analysis
      *
      * +------------------+
      * |             Total|
      * +------------------+
      * |47228.313686427464|
      * +------------------+
      */
    totalProfit.show



  }

}
