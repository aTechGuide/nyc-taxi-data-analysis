package atech.guide.Analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object PaymentTypeEvolvingWithTime {

  def apply(taxiDF: DataFrame, taxiZoneDF: DataFrame): Unit = {

    val rateCodeEvolution = taxiDF
      .groupBy(to_date(col("tpep_pickup_datetime")) as "PickUp_Day", col("RatecodeID"))
      .agg(count("*") as "TotalTrips")
      .orderBy(col("PickUp_Day"))

    rateCodeEvolution.show

  }

}
