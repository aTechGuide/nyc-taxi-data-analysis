package atech.guide.Analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object PeoplePayingForLongShortTrips {

  def apply(taxiDF: DataFrame, taxiZoneDF: DataFrame): Unit = {

    val rateCodeDistributionDF = taxiDF
      .groupBy(col("RatecodeID"))
      .agg(count("*") as "TotalTrips")
      .orderBy(col("TotalTrips").desc_nulls_last)

    /**
      * Single Day Analysis
      * +----------+----------+
      * |RatecodeID|TotalTrips|
      * +----------+----------+
      * |         1|    324387|
      * |         2|      5878|
      * |         5|       895|
      * |         3|       530|
      * |         4|       193|
      * |        99|         7|
      * |         6|         3|
      * +----------+----------+
      *
      * [ 1 (credit card), 2(cash), 3(no charge), 4 (dispute), 5 (unknown), 6 (voided)]
      *
      * Cash is Dying
      *
      * Make sure card payment processing works 24 * 7
      */

    rateCodeDistributionDF.show

  }

}
