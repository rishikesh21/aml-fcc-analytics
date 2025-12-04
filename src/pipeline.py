from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import os
import glob
import shutil
#create a spark session
spark = SparkSession.builder \
    .appName("AML_Alert_Generator") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.13:3.5.0_0.20.3") \
    .config("spark.sql.ansi.enabled", "true") \
    .getOrCreate()

#set the configs accordingly
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "400")


#global variables
INPUT_SHEET_NAME = "Input data"
OUTPUT_SHEET_NAME = "Output (Rishi)"
INPUT_FILE_PATH = "../data/Logic_TM_Test.xlsx"
OUTPUT_FILE_PATH = "../data/Logic_TM_Answer(Rishi).xlsx"
DEBUG=False

#function to read the excel data fo reach data frame
def read_excel_range(spark_session, path, sheet_name, data_address, schema):
    return (
        spark_session.read.format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("dataAddress", f"'{sheet_name}'!{data_address}")
        .option("trimHeader", "true")
        .schema(schema)
        .load(path)
    )

#define the schema so that do not have to infer schema and optimise for performance
model_schema = StructType([
    StructField("Model name", StringType(), True),
    StructField("Threshold", IntegerType(), True)
])

rule_schema = StructType([
    StructField("Model name", StringType(), True),
    StructField("Rule name", StringType(), True),
    StructField("Score", IntegerType(), True),
    StructField("Customer type", StringType(), True)
])

hit_schema = StructType([
    StructField("Rule_name", StringType(), True),
    StructField("Customer type", StringType(), True),
    StructField("Customer ID", IntegerType(), True),
    StructField("Hit Date", StringType(), True)
])


#Loading the dataframes by Excel Location
model_raw_df = read_excel_range(spark, INPUT_FILE_PATH, INPUT_SHEET_NAME, "A2:B15", model_schema)
rule_raw_df = read_excel_range(spark, INPUT_FILE_PATH, INPUT_SHEET_NAME, "D2:G30", rule_schema)
hit_raw_df = read_excel_range(spark, INPUT_FILE_PATH, INPUT_SHEET_NAME, "I2:L5426", hit_schema)

#rename and standardise column names
model_df = model_raw_df.toDF("model_name_full", "threshold")
rule_df = rule_raw_df.toDF("model_name", "rule_name", "score", "customer_type")
hit_df = hit_raw_df.toDF("rule_name", "customer_type", "customer_id", "hit_date_str")

#split the model_name_full variable into model_name and customer_type
threshold_df = (
    model_df
    .withColumn("model_base", F.split("model_name_full", "_").getItem(0))
    .withColumn(
        "customer_type",
        F.when(F.split("model_name_full", "_").getItem(1) == "INV", lit("Individual"))
         .when(F.split("model_name_full", "_").getItem(1) == "CP", lit("Corporate"))
    )
    .select(col("model_base").alias("model_name"), "customer_type", "threshold")
)

#get the counts for each df
print("threshold rows:", threshold_df.count(), "rule rows:", rule_df.count(), "hit rows:", hit_df.count())
if DEBUG:
    print("\nthreshold of models sample")
    threshold_df.orderBy("model_name", "customer_type").show(20, False)

    print("\nrules sample")
    rule_df.orderBy("model_name", "customer_type", "rule_name").show(20, False)

    print("\nrules sample")
    rule_df.orderBy("model_name", "customer_type", "rule_name").show(20, False)

# parse dates for the hits_df
hits_parsed_only = hit_df.withColumn(
    "hit_date",
    F.coalesce(
        to_date(col("hit_date_str"), "M/d/yy"),
        to_date(col("hit_date_str"), "M/d/yyyy")
    )
)

bad_dates = hits_parsed_only.filter(col("hit_date").isNull())
good_dates = hits_parsed_only.filter(col("hit_date").isNotNull())

if DEBUG:
    #get frequncy of each date so see the distribution
    hits_parsed_only.filter(col("hit_date").isNotNull()) \
    .groupBy("hit_date") \
    .count() \
    .orderBy("hit_date") \
    .show(200, False)

print("\n--- date parsing check ---")
print("raw hits:", hit_df.count(),"unparsed dates:", bad_dates.count(),"parsed dates:", good_dates.count())


if bad_dates.take(1):
    print("examples of unparsed hit_date_str values:")
    bad_dates.select("hit_date_str").groupBy("hit_date_str").count().orderBy(desc("count")).show(20, False)


# week assignment to first label weeks for all the hits
hits_wk = good_dates.withColumn(
    "week_end",
    when(F.dayofweek(col("hit_date")) == 1, col("hit_date"))
    .otherwise(next_day(col("hit_date"), "Sun"))
)

# deduplication entries to be removed and keep only first hit only per (customer, rule, week)
w = Window.partitionBy("customer_id", "rule_name", "week_end").orderBy(col("hit_date").asc())
hits_dedup = (
    hits_wk.groupBy("customer_id", "rule_name", "customer_type", "week_end")
    .agg(F.min("hit_date").alias("hit_date"))
)

if DEBUG:
    print("hits after parsing: ", hits_wk.count())
    print("hits after deduplication: ", hits_dedup.count())
    print("rows removed by deduplication : ", hits_wk.count() - hits_dedup.count())
    hits_dedup.groupBy("week_end").count().orderBy("week_end").show(5, False)

###configuration sanity checks

#sanity check 01: make sure no hits are firing that aren’t configured in the rules table
unmapped_hits = hits_dedup.join(
    rule_df.select("rule_name", "customer_type").distinct(),
    ["rule_name", "customer_type"],
    "left_anti"
)
if unmapped_hits.take(1):
    unmapped_hits.groupBy("rule_name", "customer_type").count().orderBy(desc("count")).show(50, False)
    print("Alert the operations team")


#sanity check 02: verifies that every (model_name, customer_type) that appears in the rules config is also present in threshold df
unmapped_rule_cfg = rule_df.select("model_name", "customer_type").distinct().join(
    threshold_df.select("model_name", "customer_type").distinct(),
    ["model_name", "customer_type"],
    "left_anti"
)
print("rules with no threshold:", unmapped_rule_cfg.count())
if unmapped_rule_cfg.take(1):
    unmapped_rule_cfg.limit(50).show(50, False)
    print("Alert the operations team")

##sanity check 03: compute the maximum possible score per model and customer type
max_score = (
    rule_df.select("model_name", "customer_type", "rule_name", "score").distinct()
    .groupBy("model_name", "customer_type")
    .agg(F.sum("score").alias("max_possible_score"))
)

#compare that maximum to the alert threshold
impossible = (
    threshold_df.join(max_score, ["model_name", "customer_type"], "left")
    .withColumn("max_possible_score", F.coalesce(col("max_possible_score"), lit(0)))
    .filter(col("threshold") > col("max_possible_score"))
    .select("model_name", "customer_type", "threshold", "max_possible_score")
)

if impossible.take(1):
    impossible.limit(50).show(50, False)
    print("Alert the modelling team ", impossible.count())


# perform the join and associate the score for each hit
scored = hits_dedup.join(
    F.broadcast((rule_df)),
    on=["rule_name", "customer_type"],
    how="inner"
)

#repartition by week_end to handle the weekly processing and improves parallelism
scored_for_agg = scored.repartition("week_end")
weekly_scores = (
    scored.groupBy("customer_id", "model_name", "customer_type", "week_end")
    .agg(sum(col("score")).alias("total_score"))
)


#label the alerts after joining the weekly_scores and threshold_df to check the threshold
#not broadcasting here since both the weekly_scores and threshold_df are not too big
#but if they are then can broadcast threshold df and join like the join before
alerts = (
    weekly_scores.join(threshold_df, ["model_name", "customer_type"], "inner")
    .filter(col("total_score") >= col("threshold"))
    .withColumnRenamed("week_end", "alert_date")
)

#reconstuct the model names back to the original form
final_alerts_df = alerts.select(
    col("alert_date"),
    col("customer_id"),
    concat(
        col("model_name"),
        lit("_"),
        when(col("customer_type") == "Individual", lit("INV")).otherwise(lit("CP"))
    ).alias("Model Name"),
    col("total_score"),
    col("threshold"),
    col("customer_type")
)


#alert frequency of models over the months
trends = (
    final_alerts_df
    .withColumn("MonthYear", F.date_format(col("alert_date"), "MMM-yy"))
    .groupBy("Model Name")
    .pivot("MonthYear")
    .agg(count(lit(1)))
    .na.fill(0)
)
month_cols = ["Jan-17", "Feb-17", "Mar-17", "Apr-17", "May-17", "Jun-17", "Jul-17"]
existing_month_cols = [m for m in month_cols if m in trends.columns]

trends = trends.select(["Model Name"] + existing_month_cols).orderBy("Model Name")

trends.limit(50).show(50, False)


# Rule hits overview count
rule_hits = hit_df.groupBy("rule_name").agg(count(lit(1)).alias("Number of Hits"))

#get the alerts for joining back to rules
alert_keys = final_alerts_df.select(
    "customer_id",
    "alert_date",
    F.split(col("Model Name"), "_").getItem(0).alias("model_name"),
    "customer_type"
).distinct()

#get the rule model mapping
rules_small = rule_df.select("model_name", "rule_name", "customer_type").distinct()

# alert_keys has alert_date, so rename it to week_end for joining
alerts_small = (
    alert_keys
    .withColumnRenamed("alert_date", "week_end")
    .select("customer_id", "week_end", "model_name", "customer_type")
    .distinct()
)


#in how many alerted customer weeks did this rule appear
alerts_per_rule = (
    hits_dedup.select("customer_id", "week_end", "rule_name", "customer_type")
    .join(broadcast(rules_small), ["rule_name", "customer_type"], "inner")
    .join(broadcast(alerts_small), ["customer_id", "week_end", "model_name", "customer_type"], "inner")
    .groupBy("rule_name")
    .agg(F.count(F.lit(1)).alias("Number of alerts"))
)

#generate the rule hits overview table
rule_overview = (
    rule_hits.join(alerts_per_rule, on="rule_name", how="left")
    # Fill missing alert counts with 0
    .na.fill(0, subset=["Number of alerts"])
    .orderBy("rule_name")
)

rule_overview.limit(50).show(50, False)


#run generic statistics over the derived data
top_cust = (
    final_alerts_df.groupBy("customer_id")
    .agg(count(lit(1)).alias("alert_count"))
    .orderBy(desc("alert_count"))
    .limit(1)
    .collect()
)
#prepare the results
top_text = f"Customer ID {top_cust[0]['customer_id']} ({top_cust[0]['alert_count']} alerts )"
ind_alerts = final_alerts_df.filter(col("customer_type") == "Individual").count()
corp_alerts = final_alerts_df.filter(col("customer_type") == "Corporate").count()

stats = spark.createDataFrame(
    [
        ("Which customer has the most alerts?", top_text),
        ("How many alerts was generated for Individual customers?", ind_alerts),
        ("How many alerts was generated for Corporate customers?", corp_alerts),
    ],
    ["Description", "Value"]
)
stats.show(50, False)
#write the results
#drop the July results to be incorporated for the next month
trends = trends.drop("Jul-17")
trends_sorted = trends.orderBy(col("Model Name"))
trends_sorted = (
    trends_sorted
    .withColumn("mod_num", F.regexp_extract(F.col("Model Name"), r"Mod(\d+)", 1).cast("int"))
    .withColumn("type_rank", F.when(F.col("Model Name").endswith("_INV"), F.lit(0)).otherwise(F.lit(1)))
    .orderBy("mod_num", "type_rank")
    .drop("mod_num", "type_rank")
)
#write output files
def write_csv(df, out_path):
    tmp_dir = out_path + ".__tmp"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_dir)
    part = glob.glob(os.path.join(tmp_dir, "part-*.csv"))[0]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    if os.path.exists(out_path):
        os.remove(out_path)
    os.rename(part, out_path)
    shutil.rmtree(tmp_dir)
write_csv(trends_sorted, "../data/output/alerts_trends_overview.csv")
rule_overview_sorted = rule_overview.orderBy(col("rule_name"))
write_csv(rule_overview_sorted, "../data/output/rule_hits_overview.csv")
write_csv(stats, "../data/output/general_statistics.csv")