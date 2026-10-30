import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt":os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe":os.path.join(hadoop_home, "bin", "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll":os.path.join(hadoop_home, "bin", "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\L DurgaVishnuVardhan\.jdks\corretto-1.8.0_504'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

print()


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("CustomerAnalysis").getOrCreate()

# 1. Create transaction data
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

# 2. Create customer data
customers = [
    ("C101", "Ravi"),
    ("C102", "Priya"),
    ("C103", "Kiran")
]

customers_df = spark.createDataFrame(
    customers, ["customer_id", "customer_name"]
)

# --- All operations combined into a single chained pipeline (without col) ---
final_df = (
    transactions_df
    .dropDuplicates(["transaction_id"])
    .filter("status = 'SUCCESS' and amount > 0")
    .join(customers_df, on="customer_id", how="inner")
    .groupBy("customer_id", "customer_name")
    .agg(F.sum("amount").alias("total_spending"))
    .withColumn("rank", F.dense_rank().over(Window.orderBy(F.desc("total_spending"))))
    .orderBy("rank")
)

print("=== All Customers (Single Chained Pipeline) ===")
final_df.show()
print("=====TASK2======")
#Task 2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

school_schema = StructType([
    StructField("school_id", IntegerType(), True),
    StructField("school_name", StringType(), True),
    StructField("established_year", IntegerType(), True),
    StructField("location", MapType(StringType(), StringType()), True),
    StructField("principal", StructType([
        StructField("principal_id", IntegerType(), True),
        StructField("principal_name", StringType(), True),
        StructField("email", StringType(), True)
    ]), True),
    StructField("teachers", ArrayType(StructType([
        StructField("teacher_id", IntegerType(), True),
        StructField("teacher_name", StringType(), True),
        StructField("experience", IntegerType(), True),
        StructField("salary", FloatType(), True),
        StructField("subjects", ArrayType(StringType()), True)
    ])), True),
    StructField("students", ArrayType(StructType([
        StructField("student_id", IntegerType(), True),
        StructField("student_name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("class_name", StringType(), True),
        StructField("subjects", ArrayType(StringType()), True),
        StructField("attendance", ArrayType(StructType([
            StructField("month", StringType(), True),
            StructField("total_days", IntegerType(), True),
            StructField("present_days", IntegerType(), True)
        ])), True),
        StructField("exam_results", ArrayType(StructType([
            StructField("subject", StringType(), True),
            StructField("marks", IntegerType(), True),
            StructField("grade", StringType(), True)
        ])), True)
    ])), True)
])

data = [
    (
        101,
        "Green Valley School",
        2005,
        {"city": "Hyderabad", "state": "Telangana"},
        {
            "principal_id": 9001,
            "principal_name": "Ramesh Kumar",
            "email": "ramesh@school.com"
        },
        [
            {
                "teacher_id": 501,
                "teacher_name": "Anitha",
                "experience": 8,
                "salary": 65000.0,
                "subjects": ["Maths", "Physics"]
            },
            {
                "teacher_id": 502,
                "teacher_name": "Suresh",
                "experience": 5,
                "salary": 52000.0,
                "subjects": ["Chemistry", "Biology"]
            },
            {
                "teacher_id": 503,
                "teacher_name": "Priya",
                "experience": 10,
                "salary": 72000.0,
                "subjects": ["English", "Social"]
            }
        ],
        [
            {
                "student_id": 1001,
                "student_name": "Arjun",
                "age": 15,
                "class_name": "10A",
                "subjects": ["Maths", "Physics", "English"],
                "attendance": [
                    {
                        "month": "January",
                        "total_days": 22,
                        "present_days": 20
                    },
                    {
                        "month": "February",
                        "total_days": 20,
                        "present_days": 18
                    }
                ],
                "exam_results": [
                    {
                        "subject": "Maths",
                        "marks": 92,
                        "grade": "A"
                    },
                    {
                        "subject": "Physics",
                        "marks": 85,
                        "grade": "A"
                    },
                    {
                        "subject": "English",
                        "marks": 78,
                        "grade": "B"
                    }
                ]
            },
            {
                "student_id": 1002,
                "student_name": "Sneha",
                "age": 14,
                "class_name": "9A",
                "subjects": ["Maths", "Chemistry", "English"],
                "attendance": [
                    {
                        "month": "January",
                        "total_days": 22,
                        "present_days": 16
                    },
                    {
                        "month": "February",
                        "total_days": 20,
                        "present_days": 19
                    }
                ],
                "exam_results": [
                    {
                        "subject": "Maths",
                        "marks": 88,
                        "grade": "A"
                    },
                    {
                        "subject": "Chemistry",
                        "marks": 91,
                        "grade": "A"
                    },
                    {
                        "subject": "English",
                        "marks": 82,
                        "grade": "A"
                    }
                ]
            },
            {
                "student_id": 1003,
                "student_name": "Rahul",
                "age": 15,
                "class_name": "10A",
                "subjects": ["Maths", "Physics", "Social"],
                "attendance": [
                    {
                        "month": "January",
                        "total_days": 22,
                        "present_days": 21
                    },
                    {
                        "month": "February",
                        "total_days": 20,
                        "present_days": 20
                    }
                ],
                "exam_results": [
                    {
                        "subject": "Maths",
                        "marks": 76,
                        "grade": "B"
                    },
                    {
                        "subject": "Physics",
                        "marks": 89,
                        "grade": "A"
                    },
                    {
                        "subject": "Social",
                        "marks": 95,
                        "grade": "A"
                    }
                ]
            }
        ]
    ),
    (
        102,
        "Sunrise Public School",
        2010,
        {"city": "Bengaluru", "state": "Karnataka"},
        {
            "principal_id": 9002,
            "principal_name": "Lakshmi Devi",
            "email": "lakshmi@school.com"
        },
        [
            {
                "teacher_id": 601,
                "teacher_name": "Kiran",
                "experience": 7,
                "salary": 60000.0,
                "subjects": ["Maths", "Statistics"]
            },
            {
                "teacher_id": 602,
                "teacher_name": "Meena",
                "experience": 4,
                "salary": 48000.0,
                "subjects": ["Biology", "Chemistry"]
            }
        ],
        [
            {
                "student_id": 2001,
                "student_name": "Divya",
                "age": 16,
                "class_name": "11A",
                "subjects": ["Maths", "Statistics", "Physics"],
                "attendance": [
                    {
                        "month": "January",
                        "total_days": 22,
                        "present_days": 22
                    },
                    {
                        "month": "February",
                        "total_days": 20,
                        "present_days": 17
                    }
                ],
                "exam_results": [
                    {
                        "subject": "Maths",
                        "marks": 96,
                        "grade": "A"
                    },
                    {
                        "subject": "Statistics",
                        "marks": 93,
                        "grade": "A"
                    },
                    {
                        "subject": "Physics",
                        "marks": 87,
                        "grade": "A"
                    }
                ]
            },
            {
                "student_id": 2002,
                "student_name": "Vikram",
                "age": 16,
                "class_name": "11A",
                "subjects": ["Maths", "Chemistry", "Biology"],
                "attendance": [
                    {
                        "month": "January",
                        "total_days": 22,
                        "present_days": 19
                    },
                    {
                        "month": "February",
                        "total_days": 20,
                        "present_days": 20
                    }
                ],
                "exam_results": [
                    {
                        "subject": "Maths",
                        "marks": 81,
                        "grade": "A"
                    },
                    {
                        "subject": "Chemistry",
                        "marks": 74,
                        "grade": "B"
                    },
                    {
                        "subject": "Biology",
                        "marks": 90,
                        "grade": "A"
                    }
                ]
            }
        ]
    )
]
df = spark.createDataFrame(data, school_schema)
df.printSchema()


# Flaten this data

print("=====TASK3======")
#Task 3


customers = [
    (1, "Aarav"), (2, "Vivaan"), (3, "Aditya"), (4, "Vihaan"), (5, "Arjun"),
    (6, "Sai"), (7, "Krishna"), (8, "Ishaan"), (9, "Rohan"), (10, "Karthik"),
    (11, "Rahul"), (12, "Sanjay"), (13, "Varun"), (14, "Manoj"), (15, "Nikhil"),
    (16, "Priya"), (17, "Ananya"), (18, "Sneha"), (19, "Pooja"), (20, "Kavya"),
    (21, "Divya"), (22, "Meera"), (23, "Aisha"), (24, "Ritika"), (25, "Neha"),
    (26, "Akash"), (27, "Vijay"), (28, "Surya"), (29, "Ajay"), (30, "Deepak"),
    (31, "Harish"), (32, "Ganesh"), (33, "Rakesh"), (34, "Aravind"), (35, "Praveen"),
    (36, "Bhavana"), (37, "Lakshmi"), (38, "Keerthi"), (39, "Reshma"), (40, "Swathi"),
    (41, "Anil"), (42, "Mohan"), (43, "Suresh"), (44, "Chandra"), (45, "Teja"),
    (46, "Jyothi"), (47, "Shilpa"), (48, "Ramya"), (49, "Nandhini"), (50, "Siri")
]

customer_columns = ["id", "name"]

df1 = spark.createDataFrame(customers, customer_columns)
orders = [
    (1001,1,1200),(1002,2,2500),(1003,3,1800),(1004,4,3200),(1005,5,4500),
    (1006,6,900),(1007,7,1300),(1008,8,2100),(1009,9,1700),(1010,10,5000),
    (1011,11,2400),(1012,12,3100),(1013,13,1500),(1014,14,2800),(1015,15,3900),
    (1016,16,4200),(1017,17,600),(1018,18,7000),(1019,19,1600),(1020,20,2300),
    (1021,21,3400),(1022,22,1200),(1023,23,4300),(1024,24,1750),(1025,25,2600),
    (1026,26,2200),(1027,27,3600),(1028,28,1250),(1029,29,4900),(1030,30,1800),
    (1031,31,2900),(1032,32,5400),(1033,33,1100),(1034,34,6700),(1035,35,1450),
    (1036,36,3100),(1037,37,2800),(1038,38,3500),(1039,39,4100),(1040,40,1300),
    (1041,41,2100),(1042,42,4800),(1043,43,2500),(1044,44,3900),(1045,45,1700)
]

order_columns = ["order_id", "id", "amount"]

ff1 = spark.createDataFrame(orders, order_columns)

payments = [
    (1001,"Completed"),(1002,"Pending"),(1003,"Completed"),(1004,"Failed"),(1005,"Completed"),
    (1006,"Pending"),(1007,"Completed"),(1008,"Completed"),(1009,"Failed"),(1010,"Completed"),
    (1011,"Pending"),(1012,"Completed"),(1013,"Failed"),(1014,"Completed"),(1015,"Pending"),
    (1016,"Completed"),(1017,"Failed"),(1018,"Completed"),(1019,"Pending"),(1020,"Completed"),
    (1021,"Completed"),(1022,"Failed"),(1023,"Pending"),(1024,"Completed"),(1025,"Completed"),
    (1026,"Pending"),(1027,"Completed"),(1028,"Failed"),(1029,"Completed"),(1030,"Pending"),
    (1031,"Completed"),(1032,"Completed"),(1033,"Failed"),(1034,"Pending"),(1035,"Completed"),
    (1036,"Completed"),(1037,"Pending"),(1038,"Completed"),(1039,"Failed"),(1040,"Completed"),
    (1041,"Pending"),(1042,"Completed"),(1043,"Completed"),(1044,"Failed"),(1045,"Completed")
]

payment_columns = ["order_id", "status"]

dd1 = spark.createDataFrame(payments, payment_columns)
# 1. Total sales by customer
# 2. Top 5 customers by order amount
# 3. Customers with no orders
# 4. Customers with only failed payments
# 5. Count completed, pending, failed orders
# 6. Average order value
# 7. Highest order amount customer
# 8. Customers with more than 1 order
# 9. Total revenue from completed orders
# 10. Orders above average amount



