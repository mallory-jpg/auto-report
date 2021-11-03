# "Show Me the CarF\*x": Car History Reports in Spark
A platform for tracking a given car's history of accidents, repairs, and sales after the initial sale from the dealership. It protects buyers of second-hand cars by providing a reference that informs their prospective purchases. It's kind of like a CarFax! 🦊 

This PySpark job mimic Hadoop's MapReduce functionality to produce a report of a given car's accident history by year and make. (Py)Spark has a significant advantage over traditional MapReduce's in-mem processing performance. 

### 🚙🚙 The Data 🚙🚙
Stored as a CSV in HDFS
|Column   |Type   |Info |
|---|---|---|
|incident_id   |INT   |
|incident_type   |STRING   |I=inital sale, A=accident, R=repair|
|vin_number   |STRING   |   |
|make   |STRING   |Car brand: only populated by incident type 'I'   |
|model   |STRING   |Car model: only populated by incident type 'I'   |
|year   |STRING   |Car year: only populated by incident type 'I'   |
|incident_date   |DATE   |Date of incident occurence   |
|description   |STRING   |Type of repair ('R'), details of accident ('A'), or from where the car was sold ('I')   |

## How to Use
1. Ensure data and program file are in same directory
2. Open terminal and `cd` to same directory as above
3. Run Spark job from command line: `spark-submit auto_report.py`!

Executor summary on localhost:
<img width="1419" alt="Screen Shot 2021-11-03 at 2 30 45 PM" src="https://user-images.githubusercontent.com/65197541/140179776-4226a7a2-28e3-4037-82a1-d707f352962d.png">

When your code works in Jupyter Notebooks but not in regular Python ☹️:
[spark_auto_report.pdf](https://github.com/mallory-jpg/auto-report/files/7470254/spark_auto_report.pdf)


### The Job
1. Load `csv` data into Spark dataframe. It looks like this: <img width="615" alt="Screen Shot 2021-11-03 at 2 22 44 PM" src="https://user-images.githubusercontent.com/65197541/140178634-12743921-b695-4fb1-b7c0-ad497c5316b5.png">
2. Extract `key_vin` values from df -> variable `vin_kv`
3. Perform group aggregation to populate make & year to all records -> variable `enhance_make`
4. Extract `key_make` values from RDD -> variable `make_kv` & composite keys of vehicle make+year
5. Aggregate tuples using Spark's `.reduceByKey()` to count the number of records per composite key
6. Append data to csv file
