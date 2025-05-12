# Databricks notebook source
x=1
y=2
print(x+y)

# COMMAND ----------

a = [1,2,3,4,5]
print(sum(a))

# COMMAND ----------

a = [1,2,3,4,5]
print(sum(a)/len(a))

# COMMAND ----------

b = (1,'m',0,10.0)
print(b)

# COMMAND ----------

dictt = {"name":"none","Age":97}
print(dictt)
print(dictt["Age"])
print(dictt.keys())
print(dictt.values())



# COMMAND ----------

c = {1,2,3,4,5,6}
c.add(7)
print(c)
c.update([8,9,10])
print(c)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PythonDF").getOrCreate()

data = [(1,"Alice"),(2,"Bob"),(3,"Carol")]
df = spark.createDataFrame(data,["id","name"])
# Select specific columns
df.select("name").show()

# Filter rows
df.filter(df["id"] > 1).show()

# Count rows
print(df.count())

# Describe summary statistics (numeric columns)
df.describe().show()

# Add new column with literal value
from pyspark.sql.functions import lit
df = df.withColumn("country", lit("India"))
df.show()

# COMMAND ----------

sales_data = [
    ("2024-01-01", "North", "Product A", 10, 200.0),
    ("2024-01-01", "South", "Product B", 5, 300.0),
    ("2024-01-02", "North", "Product A", 20, 400.0),
    ("2024-01-02", "South", "Product B", 10, 600.0),
    ("2024-01-03", "East",  "Product C", 15, 375.0),
]
columns = ["date", "region", "product", "quantity", "revenue"]
sales_df = spark.createDataFrame(sales_data, columns)
sales_df.show()

# COMMAND ----------

from pyspark.sql.functions import sum

sales_data = [
    ("2024-01-01", "North", "Product A", 10, 200.0),
    ("2024-01-01", "South", "Product B", 5, 300.0),
    ("2024-01-02", "North", "Product A", 20, 400.0),
    ("2024-01-02", "South", "Product B", 10, 600.0),
    ("2024-01-03", "East",  "Product C", 15, 375.0),
]
columns = ["date", "region", "product", "quantity", "revenue"]
sales_df = spark.createDataFrame(sales_data, columns)
sales_df.groupBy(columns[1]).agg(sum(columns[3])).show()



# COMMAND ----------




sales_df.groupBy("region").agg(sum("quantity").alias("total_quantity"),sum("revenue").alias("total_revenue")).show()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import avg, col

display(sales_df.groupBy("product").agg(
    (avg("revenue") / avg("quantity")).alias("average")
))


# COMMAND ----------

from pyspark.sql.functions import max

sales_df.groupBy("region").agg(
    max("revenue").alias("max_revenue")
).show()

# COMMAND ----------

sales_df.groupBy("region").sum("revenue").orderBy("sum(revenue)", ascending=False).select("region").show(1)
