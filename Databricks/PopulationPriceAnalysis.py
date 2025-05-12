# Databricks notebook source
# Load the dataset from DBFS (Databricks Filesystem)
file_path = "/databricks-datasets/samples/population-vs-price/data_geo.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.printSchema()
df.show(5)

# COMMAND ----------

# Select relevant columns for analysis
df_selected = df.select(
    "City", "State", "State Code", 
    "2014 Population estimate", "2015 median sales price"
)
df_selected.show(5)


# COMMAND ----------

# Top 10 most populated cities, sorted by population and limited to 10 rows
display(df_selected.orderBy(df_selected["2014 Population estimate"].desc()).select("City", "State", "State Code", "2014 Population estimate").limit(10))


# COMMAND ----------

from pyspark.sql.functions import avg  # Import avg function

# Group by State and calculate the average of "2015 median sales price"
df_avg_price = df_selected.groupBy("State").agg(
    avg("2015 median sales price").alias("Avg_Median_Sales_Price")
)

# Sort by the average median sales price in descending order, then limit to top 10 states
df_top10_states = df_avg_price.orderBy("Avg_Median_Sales_Price", ascending=False).limit(10)

# Show the results with State and Avg_Median_Sales_Price columns
display(df_top10_states)


# COMMAND ----------

# Display City and Population 
display(df_selected.select("City", "2014 Population estimate").orderBy("2014 Population estimate", ascending=False).limit(10))


# COMMAND ----------

# Filter for cities with missing or zero 2015 median sales price
df_missing_or_zero_price = df_selected.filter(
    (df_selected["2015 median sales price"].isNull()) | (df_selected["2015 median sales price"] == 0)
)

# Display the cities with missing or zero price values
display(df_missing_or_zero_price.select("City", "State", "2015 median sales price"))
