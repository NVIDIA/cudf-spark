/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.tests.mortgage

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// scalastyle:off line.size.limit

object GetQuarterFromCsvFileName {
  // The format is path/TYPE_yyyy\QQ.txt followed by a (_index)* where index is a single digit number [0-9]
  // i.e. mortgage/perf/Performance_2003Q4.txt_0_1
  // So we strip off the .txt and everything after it
  // and then take everything after the last remaining _
  def apply(): Column =
    substring_index(substring_index(input_file_name(), ".", 1), "_", -1)
}

object ReadPerformanceCsv {
  def apply(spark: SparkSession, path: String): DataFrame = {
    val performanceSchema = StructType(Array(
      StructField("loan_id", LongType),
      StructField("monthly_reporting_period", StringType),
      StructField("servicer", StringType),
      StructField("interest_rate", DoubleType),
      StructField("current_actual_upb", DoubleType),
      StructField("loan_age", DoubleType),
      StructField("remaining_months_to_legal_maturity", DoubleType),
      StructField("adj_remaining_months_to_maturity", DoubleType),
      StructField("maturity_date", StringType),
      StructField("msa", DoubleType),
      StructField("current_loan_delinquency_status", IntegerType),
      StructField("mod_flag", StringType),
      StructField("zero_balance_code", StringType),
      StructField("zero_balance_effective_date", StringType),
      StructField("last_paid_installment_date", StringType),
      StructField("foreclosed_after", StringType),
      StructField("disposition_date", StringType),
      StructField("foreclosure_costs", DoubleType),
      StructField("prop_preservation_and_repair_costs", DoubleType),
      StructField("asset_recovery_costs", DoubleType),
      StructField("misc_holding_expenses", DoubleType),
      StructField("holding_taxes", DoubleType),
      StructField("net_sale_proceeds", DoubleType),
      StructField("credit_enhancement_proceeds", DoubleType),
      StructField("repurchase_make_whole_proceeds", StringType),
      StructField("other_foreclosure_proceeds", DoubleType),
      StructField("non_interest_bearing_upb", DoubleType),
      StructField("principal_forgiveness_upb", StringType),
      StructField("repurchase_make_whole_proceeds_flag", StringType),
      StructField("foreclosure_principal_write_off_amount", StringType),
      StructField("servicing_activity_indicator", StringType))
    )

    spark.read.format("csv")
      .option("nullValue", "")
      .option("header", "false")
      .option("delimiter", "|")
      .option("parserLib", "univocity")
      .schema(performanceSchema)
      .load(path)
      .withColumn("quarter", GetQuarterFromCsvFileName())
  }
}

object ReadAcquisitionCsv {
  def apply(spark: SparkSession, path: String): DataFrame = {
    val acquisitionSchema = StructType(Array(
      StructField("loan_id", LongType),
      StructField("orig_channel", StringType),
      StructField("seller_name", StringType),
      StructField("orig_interest_rate", DoubleType),
      StructField("orig_upb", IntegerType),
      StructField("orig_loan_term", IntegerType),
      StructField("orig_date", StringType),
      StructField("first_pay_date", StringType),
      StructField("orig_ltv", DoubleType),
      StructField("orig_cltv", DoubleType),
      StructField("num_borrowers", DoubleType),
      StructField("dti", DoubleType),
      StructField("borrower_credit_score", DoubleType),
      StructField("first_home_buyer", StringType),
      StructField("loan_purpose", StringType),
      StructField("property_type", StringType),
      StructField("num_units", IntegerType),
      StructField("occupancy_status", StringType),
      StructField("property_state", StringType),
      StructField("zip", IntegerType),
      StructField("mortgage_insurance_percent", DoubleType),
      StructField("product_type", StringType),
      StructField("coborrow_credit_score", DoubleType),
      StructField("mortgage_insurance_type", DoubleType),
      StructField("relocation_mortgage_indicator", StringType))
    )

    spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "|")
      .schema(acquisitionSchema)
      .load(path)
      .withColumn("quarter", GetQuarterFromCsvFileName())
  }
}

object NameMapping {
  /**
   * Returns a dataframe with two columns named based off of the column names passed in.
   * The fromColName has the original name we want to clean up, the toColName
   * will have the name we want to go to, the unambiguous name.
   */
  def apply(spark: SparkSession, fromColName: String, toColName: String): DataFrame = {
    import spark.implicits._
    broadcast(Seq(
      ("WITMER FUNDING, LLC", "Witmer"),
      ("WELLS FARGO CREDIT RISK TRANSFER SECURITIES TRUST 2015", "Wells Fargo"),
      ("WELLS FARGO BANK,  NA" , "Wells Fargo"),
      ("WELLS FARGO BANK, N.A." , "Wells Fargo"),
      ("WELLS FARGO BANK, NA" , "Wells Fargo"),
      ("USAA FEDERAL SAVINGS BANK" , "USAA"),
      ("UNITED SHORE FINANCIAL SERVICES, LLC D\\/B\\/A UNITED WHOLESALE MORTGAGE" , "United Seq(e"),
      ("U.S. BANK N.A." , "US Bank"),
      ("SUNTRUST MORTGAGE INC." , "Suntrust"),
      ("STONEGATE MORTGAGE CORPORATION" , "Stonegate Mortgage"),
      ("STEARNS LENDING, LLC" , "Stearns Lending"),
      ("STEARNS LENDING, INC." , "Stearns Lending"),
      ("SIERRA PACIFIC MORTGAGE COMPANY, INC." , "Sierra Pacific Mortgage"),
      ("REGIONS BANK" , "Regions"),
      ("RBC MORTGAGE COMPANY" , "RBC"),
      ("QUICKEN LOANS INC." , "Quicken Loans"),
      ("PULTE MORTGAGE, L.L.C." , "Pulte Mortgage"),
      ("PROVIDENT FUNDING ASSOCIATES, L.P." , "Provident Funding"),
      ("PROSPECT MORTGAGE, LLC" , "Prospect Mortgage"),
      ("PRINCIPAL RESIDENTIAL MORTGAGE CAPITAL RESOURCES, LLC" , "Principal Residential"),
      ("PNC BANK, N.A." , "PNC"),
      ("PMT CREDIT RISK TRANSFER TRUST 2015-2" , "PennyMac"),
      ("PHH MORTGAGE CORPORATION" , "PHH Mortgage"),
      ("PENNYMAC CORP." , "PennyMac"),
      ("PACIFIC UNION FINANCIAL, LLC" , "Other"),
      ("OTHER" , "Other"),
      ("NYCB MORTGAGE COMPANY, LLC" , "NYCB"),
      ("NEW YORK COMMUNITY BANK" , "NYCB"),
      ("NETBANK FUNDING SERVICES" , "Netbank"),
      ("NATIONSTAR MORTGAGE, LLC" , "Nationstar Mortgage"),
      ("METLIFE BANK, NA" , "Metlife"),
      ("LOANDEPOT.COM, LLC" , "LoanDepot.com"),
      ("J.P. MORGAN MADISON AVENUE SECURITIES TRUST, SERIES 2015-1" , "JP Morgan Chase"),
      ("J.P. MORGAN MADISON AVENUE SECURITIES TRUST, SERIES 2014-1" , "JP Morgan Chase"),
      ("JPMORGAN CHASE BANK, NATIONAL ASSOCIATION" , "JP Morgan Chase"),
      ("JPMORGAN CHASE BANK, NA" , "JP Morgan Chase"),
      ("JP MORGAN CHASE BANK, NA" , "JP Morgan Chase"),
      ("IRWIN MORTGAGE, CORPORATION" , "Irwin Mortgage"),
      ("IMPAC MORTGAGE CORP." , "Impac Mortgage"),
      ("HSBC BANK USA, NATIONAL ASSOCIATION" , "HSBC"),
      ("HOMEWARD RESIDENTIAL, INC." , "Homeward Mortgage"),
      ("HOMESTREET BANK" , "Other"),
      ("HOMEBRIDGE FINANCIAL SERVICES, INC." , "HomeBridge"),
      ("HARWOOD STREET FUNDING I, LLC" , "Harwood Mortgage"),
      ("GUILD MORTGAGE COMPANY" , "Guild Mortgage"),
      ("GMAC MORTGAGE, LLC (USAA FEDERAL SAVINGS BANK)" , "GMAC"),
      ("GMAC MORTGAGE, LLC" , "GMAC"),
      ("GMAC (USAA)" , "GMAC"),
      ("FREMONT BANK" , "Fremont Bank"),
      ("FREEDOM MORTGAGE CORP." , "Freedom Mortgage"),
      ("FRANKLIN AMERICAN MORTGAGE COMPANY" , "Franklin America"),
      ("FLEET NATIONAL BANK" , "Fleet National"),
      ("FLAGSTAR CAPITAL MARKETS CORPORATION" , "Flagstar Bank"),
      ("FLAGSTAR BANK, FSB" , "Flagstar Bank"),
      ("FIRST TENNESSEE BANK NATIONAL ASSOCIATION" , "Other"),
      ("FIFTH THIRD BANK" , "Fifth Third Bank"),
      ("FEDERAL HOME LOAN BANK OF CHICAGO" , "Fedral Home of Chicago"),
      ("FDIC, RECEIVER, INDYMAC FEDERAL BANK FSB" , "FDIC"),
      ("DOWNEY SAVINGS AND LOAN ASSOCIATION, F.A." , "Downey Mortgage"),
      ("DITECH FINANCIAL LLC" , "Ditech"),
      ("CITIMORTGAGE, INC." , "Citi"),
      ("CHICAGO MORTGAGE SOLUTIONS DBA INTERFIRST MORTGAGE COMPANY" , "Chicago Mortgage"),
      ("CHICAGO MORTGAGE SOLUTIONS DBA INTERBANK MORTGAGE COMPANY" , "Chicago Mortgage"),
      ("CHASE HOME FINANCE, LLC" , "JP Morgan Chase"),
      ("CHASE HOME FINANCE FRANKLIN AMERICAN MORTGAGE COMPANY" , "JP Morgan Chase"),
      ("CHASE HOME FINANCE (CIE 1)" , "JP Morgan Chase"),
      ("CHASE HOME FINANCE" , "JP Morgan Chase"),
      ("CASHCALL, INC." , "CashCall"),
      ("CAPITAL ONE, NATIONAL ASSOCIATION" , "Capital One"),
      ("CALIBER HOME LOANS, INC." , "Caliber Funding"),
      ("BISHOPS GATE RESIDENTIAL MORTGAGE TRUST" , "Bishops Gate Mortgage"),
      ("BANK OF AMERICA, N.A." , "Bank of America"),
      ("AMTRUST BANK" , "AmTrust"),
      ("AMERISAVE MORTGAGE CORPORATION" , "Amerisave"),
      ("AMERIHOME MORTGAGE COMPANY, LLC" , "AmeriHome Mortgage"),
      ("ALLY BANK" , "Ally Bank"),
      ("ACADEMY MORTGAGE CORPORATION" , "Academy Mortgage"),
      ("NO CASH-OUT REFINANCE" , "OTHER REFINANCE"),
      ("REFINANCE - NOT SPECIFIED" , "OTHER REFINANCE"),
      ("Other REFINANCE" , "OTHER REFINANCE")
    ).toDF(fromColName, toColName))
  }
}

object CreatePerformanceDelinquency {
  def prepare(df: DataFrame): DataFrame = {
    df
      .withColumn("monthly_reporting_period", to_date(col("monthly_reporting_period"), "MM/dd/yyyy"))
      .withColumn("monthly_reporting_period_month", month(col("monthly_reporting_period")))
      .withColumn("monthly_reporting_period_year", year(col("monthly_reporting_period")))
      .withColumn("monthly_reporting_period_day", dayofmonth(col("monthly_reporting_period")))
      .withColumn("last_paid_installment_date", to_date(col("last_paid_installment_date"), "MM/dd/yyyy"))
      .withColumn("foreclosed_after", to_date(col("foreclosed_after"), "MM/dd/yyyy"))
      .withColumn("disposition_date", to_date(col("disposition_date"), "MM/dd/yyyy"))
      .withColumn("maturity_date", to_date(col("maturity_date"), "MM/yyyy"))
      .withColumn("zero_balance_effective_date", to_date(col("zero_balance_effective_date"), "MM/yyyy"))
      .withColumn("current_actual_upb", col("current_actual_upb"))
      .withColumn("current_loan_delinquency_status", col("current_loan_delinquency_status"))
  }

  def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    val aggDF = df
      .select(
        col("quarter"),
        col("loan_id"),
        col("current_loan_delinquency_status"),
        when(col("current_loan_delinquency_status") >= 1, col("monthly_reporting_period")).alias("delinquency_30"),
        when(col("current_loan_delinquency_status") >= 3, col("monthly_reporting_period")).alias("delinquency_90"),
        when(col("current_loan_delinquency_status") >= 6, col("monthly_reporting_period")).alias("delinquency_180")
      )
      .groupBy("quarter", "loan_id")
      .agg(
        max("current_loan_delinquency_status").alias("delinquency_12"),
        min("delinquency_30").alias("delinquency_30"),
        min("delinquency_90").alias("delinquency_90"),
        min("delinquency_180").alias("delinquency_180")
      )
      .select(
        col("quarter"),
        col("loan_id"),
        (col("delinquency_12") >= 1).alias("ever_30"),
        (col("delinquency_12") >= 3).alias("ever_90"),
        (col("delinquency_12") >= 6).alias("ever_180"),
        col("delinquency_30"),
        col("delinquency_90"),
        col("delinquency_180")
      )

    val joinedDf = df
      .withColumnRenamed("monthly_reporting_period", "timestamp")
      .withColumnRenamed("monthly_reporting_period_month", "timestamp_month")
      .withColumnRenamed("monthly_reporting_period_year", "timestamp_year")
      .withColumnRenamed("current_loan_delinquency_status", "delinquency_12")
      .withColumnRenamed("current_actual_upb", "upb_12")
      .select("quarter", "loan_id", "timestamp", "delinquency_12", "upb_12", "timestamp_month", "timestamp_year")
      .join(aggDF, Seq("loan_id", "quarter"), "left_outer")

    // calculate the 12 month delinquency and upb values
    val months = 12
    val monthArray = 0.until(12).toArray
    val testDf = joinedDf
      // explode on a small amount of data is actually slightly more efficient than a cross join
      .withColumn("month_y", explode(lit(monthArray)))
      .select(
        col("quarter"),
        floor(((col("timestamp_year") * 12 + col("timestamp_month")) - 24000) / months).alias("josh_mody"),
        floor(((col("timestamp_year") * 12 + col("timestamp_month")) - 24000 - col("month_y")) / months).alias("josh_mody_n"),
        col("ever_30"),
        col("ever_90"),
        col("ever_180"),
        col("delinquency_30"),
        col("delinquency_90"),
        col("delinquency_180"),
        col("loan_id"),
        col("month_y"),
        col("delinquency_12"),
        col("upb_12")
      )
      .groupBy("quarter", "loan_id", "josh_mody_n", "ever_30", "ever_90", "ever_180", "delinquency_30", "delinquency_90", "delinquency_180", "month_y")
      .agg(max("delinquency_12").alias("delinquency_12"), min("upb_12").alias("upb_12"))
      .withColumn("timestamp_year", floor((lit(24000) + (col("josh_mody_n") * lit(months)) + (col("month_y") - 1)) / lit(12)))
      .withColumn("timestamp_month_tmp", pmod(lit(24000) + (col("josh_mody_n") * lit(months)) + col("month_y"), lit(12)))
      .withColumn("timestamp_month", when(col("timestamp_month_tmp") === lit(0), lit(12)).otherwise(col("timestamp_month_tmp")))
      .withColumn("delinquency_12", ((col("delinquency_12") > 3).cast("int") + (col("upb_12") === 0).cast("int")).alias("delinquency_12"))
      .drop("timestamp_month_tmp", "josh_mody_n", "month_y")

    df.withColumnRenamed("monthly_reporting_period_month", "timestamp_month")
      .withColumnRenamed("monthly_reporting_period_year", "timestamp_year")
      .join(testDf, Seq("quarter", "loan_id", "timestamp_year", "timestamp_month"), "left").drop("timestamp_year", "timestamp_month")
  }
}

object CreateAcquisition {
  def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    val nameMapping = NameMapping(spark, "from_seller_name", "to_seller_name")
    df.join(nameMapping, col("seller_name") === col("from_seller_name"), "left")
      .drop("from_seller_name")
      // backup the original name before we replace it
      .withColumn("old_name", col("seller_name"))
      // replace seller_name with the new version if we found one in the mapping, or the old version
      // if we didn't
      .withColumn("seller_name", coalesce(col("to_seller_name"), col("seller_name")))
      .drop("to_seller_name")
      .withColumn("orig_date", to_date(col("orig_date"), "MM/yyyy"))
      .withColumn("first_pay_date", to_date(col("first_pay_date"), "MM/yyyy"))
  }
}

object CleanAcquisitionPrime {
  def apply(spark: SparkSession, perfDF: DataFrame, acqDF: DataFrame): DataFrame = {
    val perf = CreatePerformanceDelinquency(spark, perfDF)
    val acq = CreateAcquisition(spark, acqDF)

    perf.join(acq, Seq("loan_id", "quarter"), "inner").drop("quarter")
  }
}

object Run {
  def csv(spark: SparkSession, mortgagePerformance: String, mortgageAcquisition: String): DataFrame = {
    val dfPerf = CreatePerformanceDelinquency.prepare(ReadPerformanceCsv(spark, mortgagePerformance))
    val dfAcq = ReadAcquisitionCsv(spark, mortgageAcquisition)

    CleanAcquisitionPrime(spark, dfPerf, dfAcq)
  }

  def parquet(spark: SparkSession, mortgagePerformance: String, mortgageAcquisition: String): DataFrame = {
    val dfPerf = CreatePerformanceDelinquency.prepare(spark.read.parquet(mortgagePerformance))
    val dfAcq = spark.read.parquet(mortgageAcquisition)

    CleanAcquisitionPrime(spark, dfPerf, dfAcq)
  }

  def orc(spark: SparkSession, mortgagePerformance: String, mortgageAcquisition: String): DataFrame = {
    val dfPerf = CreatePerformanceDelinquency.prepare(spark.read.orc(mortgagePerformance))
    val dfAcq = spark.read.orc(mortgageAcquisition)

    CleanAcquisitionPrime(spark, dfPerf, dfAcq)
  }
}

object SimpleAggregates {

  def csv(spark: SparkSession, mortgagePerformance: String, mortgageAcquisition: String): DataFrame = {
    val dfp = ReadPerformanceCsv(spark, mortgagePerformance)
    val dfa = ReadAcquisitionCsv(spark, mortgageAcquisition)

    val maxRate = dfp
      .withColumn("monthly_reporting_period", to_date(col("monthly_reporting_period"), "MM/dd/yyyy"))
      .withColumn("monthval", month(col("monthly_reporting_period")))
      .groupBy(col("monthval"), col("loan_id"))
      .agg(max("interest_rate").as("max_monthly_rate"))

    val joined = maxRate.join(dfa, maxRate("loan_id") === dfa("loan_id"))
    joined.groupBy(col("zip"), col("monthval"))
        .agg(min("max_monthly_rate").as("min_max_monthly_rate"))
  }
}

object AggregatesWithPercentiles {
  def csv(spark: SparkSession, mortgagePerformance: String): DataFrame = {
    val dfp =  ReadPerformanceCsv(spark, mortgagePerformance)
    val kindOfAnon = dfp.withColumn("loan_id_hash", hex(hash(col("loan_id"))))
      .drop("loan_id")

    kindOfAnon.createOrReplaceTempView("perf_data")

    spark.sql(
      """
        select r.loan_id_hash,
          round(min(r.interest_rate), 4) as interest_rate_min,
          round(max(r.interest_rate), 4) as interest_rate_max,
          round(avg(r.interest_rate), 4) as interest_rate_avg,
          round(percentile(r.interest_rate, 0.5), 4) as interest_rate_50p,
          round(percentile(r.interest_rate, 0.75), 4) as interest_rate_75p,
          round(percentile(r.interest_rate, 0.90), 4) as interest_rate_90p,
          round(percentile(r.interest_rate, 0.99), 4) as interest_rate_99p
        from perf_data r
         group by r.loan_id_hash
      """.stripMargin)
  }
}

object AggregatesWithJoin {
  def csv(spark: SparkSession, mortgagePerformance: String, mortgageAcquisition: String): DataFrame = {
    val dfp =  ReadPerformanceCsv(spark, mortgagePerformance)
    val kindOfAnonPerf = dfp.withColumn("loan_id_hash", hex(hash(col("loan_id"))))
      .drop("loan_id")

    kindOfAnonPerf.createOrReplaceTempView("perf_data")

    val dfa =  ReadAcquisitionCsv(spark, mortgageAcquisition)
    val kindOfAnonAcq = dfa.withColumn("loan_id_hash", hex(hash(col("loan_id"))))
      .drop("loan_id")

    kindOfAnonAcq.createOrReplaceTempView("acq_data")

    spark.sql(
      """
        select * from (
          select loan_id_hash,
           min(interest_rate) as min_int_rate
          from perf_data
          group by loan_id_hash
        ) a left join (
          select loan_id_hash,
           first(orig_interest_rate) as first_int_rate,
           coalesce(max(dti), 0.0) as max_dti
          from acq_data
          group by loan_id_hash
        ) b on a.loan_id_hash = b.loan_id_hash
      """.stripMargin)
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length < 4 || args.length > 5) {
        System.err.println("Usage:<sparkversion> <perfpath> <acqpath> <outputpath> [csv|orc|parquet]")
        System.exit(1)
    }
    val perfPath = args(1)
    val acqPath = args(2)
    val output = args(3)

    val session = SparkSession.builder
      .appName("MortgageJob")
      .getOrCreate()

    // extend args to support csv/orc/parquet dataset
    val dataFrameFormatMap = Map(
      "csv" -> { () => Run.csv(session, perfPath, acqPath) },
      "orc" -> { () => Run.orc(session, perfPath, acqPath) },
      "parquet" -> { () => Run.parquet(session, perfPath, acqPath) }
    )
    val format = args.lift(4).getOrElse("parquet")
    val contains = dataFrameFormatMap.contains(format)
    if (!contains) {
        System.err.println(s"Invalid input format $format, expected one of csv, orc, parquet")
        System.exit(1)
    }

    val runFunc = dataFrameFormatMap(format)
    0.until(10).foreach( _ => runFunc().write.mode("overwrite").parquet(output))
  }
}
