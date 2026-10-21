from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example").getOrCreate()

data = [
    ("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),
    ("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),
    ("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),
    ("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),
    ("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")
]
df = (spark.createDataFrame(data)
      .toDF("workerid", "firstname", "lastname", "salary", "joiningdate", "depart"))

df.show()

a = df.alias("a")
b = df.alias("b")

finaldf = (a
           .join(b, (a["salary"] == b["salary"]) & (a["workerid"] != b["workerid"]), "inner")
           .select("a.*"))
finaldf.show()



print(finaldf)