
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
spark = SparkSession.builder.getOrCreate()
transactions = [
    ("T101", "C101", 1000, "SUCCESS"),
    ("T102", "C102", 2000, "SUCCESS"),
    ("T103", "C101", 500, "FAILED"),
    ("T104", "C103", 1500, "SUCCESS"),
    ("T105", "C101", 700, "SUCCESS"),
    ("T106", "C102", 2000, "SUCCESS"),
    ("T107", "C104", -100, "SUCCESS")
]

transaction_cols = [
    "transaction_id", "customer_id", "amount", "status"
]

transactions_df = spark.createDataFrame(
    transactions, transaction_cols
)
transactions_df.show()


customers = [
    ("C101", "Ravi"),
    ("C102", "Priya"),
    ("C103", "Kiran")
]

customers_df = spark.createDataFrame(
    customers, ["customer_id", "customer_name"]
)
customers_df.show()


success= transactions_df.filter("status = 'SUCCESS'").dropDuplicates(["transaction_id"])
success.show()

customer = (success
           .join(customers_df, "customer_id", "inner")
           .groupBy("customer_id", "customer_name")
           .agg(sum("amount").alias("total_amount"))
           )
win = Window.orderBy(desc("total_amount"))
ranked = customer.withColumn("rank", rank().over(win))
ranked.show()
