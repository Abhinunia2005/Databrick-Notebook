# Databricks notebook source
# MAGIC %md
# MAGIC # 📝 Assignment: Python Collections
# MAGIC This notebook contains practice exercises on Python collections like List, Dictionary, Tuple, and Set.

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Part 1: List Operations
# MAGIC Given the list of sales amounts:

# COMMAND ----------

sales = [250, 300, 400, 150, 500, 200]

# COMMAND ----------

# MAGIC %md
# MAGIC **Tasks:**
# MAGIC 1. Find the total sales.
# MAGIC 2. Calculate the average sale amount.
# MAGIC 3. Print sale values above 300.
# MAGIC 4. Add `350` to the list.
# MAGIC 5. Sort the list in descending order.

# COMMAND ----------

sales = [250, 300, 400, 150, 500, 200]

# 1. Total sales
total_sales = sum(sales)
print("Total Sales:", total_sales)

# 2. Average sale amount
average_sale = total_sales / len(sales)
print("Average Sale Amount:", average_sale)

# 3. Sales above 300
above_300 = [sale for sale in sales if sale > 300]
print("Sales Above 300:", above_300)

# 4. Add 350 to the list
sales.append(350)
print("Updated Sales:", sales)

# 5. Sort in descending order
sales.sort(reverse=True)
print("Sales in Descending Order:", sales)


# COMMAND ----------

# MAGIC %md
# MAGIC ##  Part 2: Dictionary Operations
# MAGIC Create a dictionary with product names and their prices:

# COMMAND ----------

products = {
    "Laptop": 70000,
    "Mouse": 500,
    "Keyboard": 1500,
    "Monitor": 12000
}

# COMMAND ----------

products = {
    "Laptop": 70000,
    "Mouse": 500,
    "Keyboard": 1500,
    "Monitor": 12000
}

# 1. Print the price of the "Monitor"
print("Price of Monitor:", products["Monitor"])

# 2. Add "Webcam" with price 3000
products["Webcam"] = 3000

# 3. Update the price of "Mouse" to 550
products["Mouse"] = 550

# 4. Print all product names and prices
print("Updated Products List:")
for name, price in products.items():
    print(f"{name}: ₹{price}")


# COMMAND ----------

# MAGIC %md
# MAGIC **Tasks:**
# MAGIC 1. Print the price of the "Monitor".
# MAGIC 2. Add a new product `"Webcam"` with price `3000`.
# MAGIC 3. Update the price of "Mouse" to `550`.
# MAGIC 4. Print all product names and prices using a loop.