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


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ###################################

print("=====TASK ANS====")
#Task1

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



#Task2
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






explode_df=(df
            .withColumn("teacher", explode(col("teachers")))
            .withColumn("student", explode(col("students")))
            .withColumn("attendance", explode(col("student.attendance")))
            .withColumn("exam_result", explode(col("student.exam_results")))
            )
print("=== Exploded DataFrame ===")
explode_df.show(truncate=False)
explode_df.printSchema()

tol_flatten= (explode_df
              .withColumn("city", col("location")["city"])
              .withColumn("state", col("location")["state"])
              .withColumn("principal_id", col("principal.principal_id"))
              .withColumn("principal_name", col("principal.principal_name"))
              .withColumn("principal_email", col("principal.email"))
              .withColumn("teacher_id", col("teacher.teacher_id"))
              .withColumn("teacher_name", col("teacher.teacher_name"))
              .withColumn("teacher_experience", col("teacher.experience"))
              .withColumn("teacher_salary", col("teacher.salary"))
              .withColumn("teacher_subjects", col("teacher.subjects"))
              .withColumn("student_id", col("student.student_id"))
              .withColumn("student_name", col("student.student_name"))
              .withColumn("student_age", col("student.age"))
              .withColumn("class_name", col("student.class_name"))
              .withColumn("student_subjects", col("student.subjects"))
              .withColumn("attendance_month", col("attendance.month"))
              .withColumn("attendance_total_days", col("attendance.total_days"))
              .withColumn("attendance_present_days", col("attendance.present_days"))
              .withColumn("exam_subject", col("exam_result.subject"))
              .withColumn("exam_marks", col("exam_result.marks"))
              .withColumn("exam_grade", col("exam_result.grade"))
              .select("school_id", "city", "state", "principal_id", "principal_name", "principal_email",
                      "teacher_id", "teacher_name", "teacher_experience", "teacher_salary", "teacher_subjects",
                      "student_id", "student_name", "student_age", "class_name", "student_subjects",
                      "attendance_month", "attendance_total_days", "attendance_present_days",
                      "exam_subject", "exam_marks", "exam_grade")
              )
print("=== Flattened DataFrame ===")
tol_flatten.show(truncate=False)
tol_flatten.printSchema()

expo=(tol_flatten
      .withColumn("teacher_subjects",explode(col("teacher_subjects")))
      .withColumn("student_subjects",explode(col("student_subjects")))
      )
print("=== Exploded DataFrame ===")
expo.show(truncate=False)
expo.printSchema()
