from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import input_file_name, regexp_extract
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("AIMCo_Silver_Transformation") \
    .getOrCreate()

def run_transformation():
    load_date = "2026-01-09"
    input_path = f"data/bronze/{load_date}/*.csv"
    output_path = f"data/silver/{load_date}/stock_analytic.parquet"

    print(f"Reading Bronze data from: {input_path}")

    # Read the data AND extract the Ticker from the filename
    raw_df = spark.read.csv(input_path, header=True, inferSchema=True) \
        .withColumn("filename", input_file_name()) \
        .withColumn("Ticker", regexp_extract("filename", r"([^/]+)_raw\.csv$", 1))

    # Define the Window (Now Spark knows what 'Ticker' is!)
    window_spec = Window.partitionBy("Ticker").orderBy("Date")

    # Perform Calculations
    silver_df = raw_df \
        .withColumn("Daily_Return", (F.col("Close") / F.lag("Close").over(window_spec)) - 1) \
        .withColumn("7_Day_MA", F.avg("Close").over(window_spec.rowsBetween(-6, 0))) \
        .withColumn("7_Day_Vol", F.stddev("Daily_Return").over(window_spec.rowsBetween(-6, 0)))

    # Clean up and Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    silver_df.drop("filename").write.mode("overwrite").parquet(output_path)

    print(f" Silver Layer created successfully!")
    silver_df.select("Date", "Ticker", "Close", "7_Day_MA").show(10)

if __name__ == "__main__":
    run_transformation()