# Databricks notebook source
# MAGIC %md
# MAGIC # Flipkart EDA assignment
# MAGIC
# MAGIC ## TODO: Upload csv file before moving to next

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Flipkart Data Engineering").getOrCreate()
file_path = '/FileStore/tables/Flipkart.csv'
flipkart_df = spark.read.csv(file_path, header=True, inferSchema=True)

# COMMAND ----------

flipkart_df.printSchema()
flipkart_df.count()

# COMMAND ----------

flipkart_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# Display all category names
flipkart_df.select("maincateg").distinct().show()


# COMMAND ----------

# Filter products with rating > 4.5 and more than 1000 reviews
flipkart_df.filter((flipkart_df["Rating"] > 4) & (flipkart_df["noreviews1"] > 100)) \
    .select("title", "Rating", "noreviews1").show()


# COMMAND ----------

# Display Products in 'Men' category that are fulfilled
flipkart_df.filter((flipkart_df["maincateg"] == "Men") & (flipkart_df["fulfilled1"] == 1)) \
    .select("title", "maincateg", "fulfilled1").show()


# COMMAND ----------

# Display number of products per category
flipkart_df.groupBy("maincateg").count().show()


# COMMAND ----------

# Display Average rating per category
flipkart_df.groupBy("maincateg").agg({"Rating": "avg"}).show()


# COMMAND ----------

# Display Category with highest average number of reviews
flipkart_df.groupBy("maincateg").agg({"noreviews1": "avg"}) \
    .orderBy("avg(noreviews1)", ascending=False).show(1)


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# Top 5 products with highest price. Display product name and price
flipkart_df.select("title", "actprice1").orderBy("actprice1", ascending=False).limit(5).show()


# COMMAND ----------

# Display Min, max, and avg price per category
flipkart_df.groupBy("maincateg").agg(
    {"actprice1": "min", "actprice1": "max", "actprice1": "avg"}
).show()


# COMMAND ----------

# Display number of nulls in each column
from pyspark.sql.functions import col, sum
flipkart_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in flipkart_df.columns]).show()


# COMMAND ----------

# Calculate and display the category name, number of fulfilled, and unfulfilled products
flipkart_df.groupBy("maincateg", "fulfilled1").count().show()


# COMMAND ----------

# Display Count of products per category
flipkart_df.groupBy("maincateg").count().show()


# COMMAND ----------

# Display Average rating per category
flipkart_df.groupBy("maincateg").agg({"Rating": "avg"}).show()


# COMMAND ----------

# Display Category with highest average number of reviews
flipkart_df.groupBy("maincateg").agg({"noreviews1": "avg"}) \
    .orderBy("avg(noreviews1)", ascending=False).show(1)


# COMMAND ----------

# Bar chart of product count per category
category_counts = flipkart_df.groupBy("maincateg").count()

# Use Databricks UI to visualize this as a bar chart
category_counts.display()


# COMMAND ----------

# Bar chart of average rating per category
category_avg_rating = flipkart_df.groupBy("maincateg").agg({"Rating": "avg"})

# Use Databricks UI to visualize this as a bar chart
category_avg_rating.display()


# COMMAND ----------

# Bar chart of total number of reviews per category
category_reviews = flipkart_df.groupBy("maincateg").agg({"noreviews1": "sum"})

# Use Databricks UI to visualize this as a bar chart
category_reviews.display()


# COMMAND ----------

# Display product name and 5-star rating for those products with the highest 5-star rating
display(flipkart_df.filter(flipkart_df["star_5f"] > 0).select("title", "star_5f").orderBy("star_5f", ascending=False))
