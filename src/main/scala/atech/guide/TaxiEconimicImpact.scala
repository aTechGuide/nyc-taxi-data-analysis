package atech.guide

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TaxiEconimicImpact {

  /**
    * 1 - big data source [localhost -> "data/NYC_taxi_2009-2016.parquet"]
    * 2 - taxi zones data source [localhost -> "src/main/resources/data/taxi_zones.csv"]
    * 3 - output data destination [localhost -> output]
    */
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      System.exit(1)
    }


    val spark: SparkSession = createSparkSession

    import spark.implicits._

    val bigTaxiDF = spark.read.load(args(0))

    val taxiZoneDF = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(args(1))

    // Business Parameters

    val percentGroupAttempt = 0.05
    val percentAcceptGrouping = 0.3
    val discount = 5
    val extraCost = 2
    val averageCostReduction = 0.6 * bigTaxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)
    val percentGroupable = 289623 * 1.0 / 331893 // <- Based on Single day data. As bigTaxiDF contains Nulls for `passenger_count`


    // All the trips that can be grouped by location ID and 5 mins bucket.
    val groupAttemptsDF = bigTaxiDF
      .select(
        round(unix_timestamp(col("pickup_datetime")) / 300 ).cast("integer") as "FiveMinId",
        col("pickup_taxizone_id"),
        col("total_amount")
      )
      // .where(col("passenger_count") < 3) // <- This filter can NOT be done. As column has NULLs
      .groupBy(col("FiveMinId"), col("pickup_taxizone_id")) // <- Grouping the trip starting from same location within 5 min time window
      .agg((count("*") * percentGroupable) as "TotalTrips", sum(col("total_amount")) as "TotalAmount")
      .orderBy(col("TotalTrips").desc_nulls_last)
      .withColumn("Approx_Datetime", from_unixtime(col("FiveMinId") * 300))
      .drop("FiveMinId")
      .join(taxiZoneDF, col("pickup_taxizone_id") === col("LocationID"))
      .drop("LocationID", "service_zone")


    val groupingExtimatedEconomicDF = groupAttemptsDF
      .withColumn("groupedRides", col("TotalTrips") * percentGroupAttempt)
      .withColumn("acceptedGroupedRidesEconomucImpact", col("groupedRides") * percentAcceptGrouping * (averageCostReduction - discount))
      .withColumn("rejectedGroupedRidesEconomicRides", col("groupedRides") * (1 - percentGroupAttempt) * extraCost)
      .withColumn("totalImpact", col("acceptedGroupedRidesEconomucImpact") + col("rejectedGroupedRidesEconomicRides"))


    val totalEconomicImpact = groupingExtimatedEconomicDF.select(sum(col("totalImpact")) as "Total").cache


    /**
      * +-------------------+
      * |              Total|
      * +-------------------+
      * |1.690068913412097E8|
      * +-------------------+
      */
    totalEconomicImpact.show

    totalEconomicImpact.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(args(2))

  }

  def createSparkSession: SparkSession = {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

}
