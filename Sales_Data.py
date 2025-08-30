# Databricks notebook source
spark

# COMMAND ----------


#/FileStore/tables/sales.csv
#/FileStore/tables/menu.csv


# COMMAND ----------

from pyspark.sql.types import *
schema = StructType([
    StructField("ProductID", IntegerType(), True),
    StructField("CustomerId", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("location", StringType(), True),
    StructField("source_order", StringType(), True)
])

sales_df = spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/sales.csv")
display(sales_df)


# COMMAND ----------

from pyspark.sql.functions import quarter, month, year

sales_df = sales_df.withColumn("Order_Year",year(sales_df.order_date)).withColumn("Order_Month",month(sales_df.order_date)).withColumn("Order_Quarter",quarter(sales_df.order_date))
display(sales_df)

# COMMAND ----------

from pyspark.sql.functions import *
menuSchema = StructType([
    StructField("ProductID", IntegerType(), True),
    StructField("Product_Name", StringType(), True),
    StructField("Price", StringType(), True)
])

menu_df = spark.read.format("csv").option("inferschema","true").schema(menuSchema).load("/FileStore/tables/menu.csv")

menu_df.select(col("Price").cast('int').alias("Price"))
display(menu_df)


# COMMAND ----------

# DBTITLE 1,Total Amount spent by each customer
sales_df.createOrReplaceTempView("sales")
menu_df.createOrReplaceTempView("menu")

df = spark.sql("select * from sales inner join menu on sales.ProductID=menu.ProductID")
df.createOrReplaceTempView("combo")
spark.sql("select CustomerId, sum(Price) from combo group by CustomerID order by customerID").show()

df1 = (sales_df.join(menu_df,"ProductID")).groupBy("CustomerId").agg({'Price':'sum'}).orderBy("CustomerId")
df1.display()

# COMMAND ----------

# DBTITLE 1,Total amount spent by each food category
df2 = (sales_df.join(menu_df,'ProductID')).groupBy("Product_Name").agg({'Price':'sum'}).orderBy('Product_Name')
display(df2)

# COMMAND ----------

# DBTITLE 1,Total Amount of Sales in each month
df3 = (sales_df.join(menu_df,'ProductID')).groupBy('Order_Month').agg({'Price':'sum'}).orderBy('Order_Month')
display(df3)

# COMMAND ----------

# DBTITLE 1,Yearly sales
df4 = (sales_df.join(menu_df,'ProductID')).groupBy('Order_Year').agg({'Price':'sum'}).orderBy('Order_Year')
display(df4)

# COMMAND ----------

# DBTITLE 1,quarterly sales
df5 = (sales_df.join(menu_df,'ProductID')).groupBy('Order_Quarter').agg({'Price':'sum'}).orderBy('Order_Quarter')
display(df5)

# COMMAND ----------

# DBTITLE 1,how many times each product has been purchased
df6 = (sales_df.join(menu_df,'ProductID')).groupBy('ProductID','Product_Name').agg(count('ProductID').alias('Product_Count')).orderBy('Product_Count', ascending=0)
display(df6)

# COMMAND ----------

# DBTITLE 1,Top 5 ordered items
df7 = (sales_df.join(menu_df,'ProductID')).groupBy('ProductID','Product_Name').agg(count('ProductID').alias('Product_Count')).orderBy('Product_Count', ascending=0).limit(5)
display(df7)


# COMMAND ----------

# DBTITLE 1,Top ordered items
df8 = (sales_df.join(menu_df,'ProductID')).groupBy('ProductID','Product_Name').agg(count('ProductID').alias('Product_Count')).orderBy('Product_Count', ascending=0).limit(1)
display(df8)

# COMMAND ----------

# DBTITLE 1,Frequency of customer visited to restaurant
df9 = (sales_df.filter(sales_df.source_order == 'Restaurant').groupBy('CustomerID').agg(countDistinct('order_date')))

display(df9)

# COMMAND ----------


