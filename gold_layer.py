from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("StockGoldLayer").getOrCreate()

# 1. Load the Silver Data
silver_df = spark.read.parquet("data/silver/2026-01-09/stock_analytic.parquet")

# 2. Aggregation: Volatility Rank (Risk Report)
# We calculate the mean volatility for each stock to see which is "riskiest"
risk_report = silver_df.groupBy("Ticker").agg(
    F.mean("7_Day_Vol").alias("Avg_Weekly_Volatility"),
    F.max("Close").alias("Year_High"),
    F.min("Close").alias("Year_Low")
).orderBy(F.desc("Avg_Weekly_Volatility"))

# 3. Business Logic: Simple Moving Average (SMA) Crossover Signals
# Strategy: If 7-day MA > 21-day MA, it's a "Bullish" trend.
# Note: For a real 21-day MA, you'd add that in the Silver layer first. 
# Here we'll use a simple logic: If Close > 7_Day_MA, then 'BUY'
trading_signals = silver_df.withColumn(
    "Signal",
    F.when(F.col("Close") > F.col("7_Day_MA"), "BUY")
    .otherwise("SELL")
)

# 4. Save to Gold Folder
risk_report.write.mode("overwrite").parquet("data/gold_risk_report")
trading_signals.write.mode("overwrite").parquet("data/gold_trading_signals")

print("Gold Layer Created Successfully!")
risk_report.show()