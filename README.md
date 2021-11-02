# "Show Me the CarF\*x": Car History Reports in Spark
A platform for tracking a given car's history of accidents, repairs, and sales after the initial sale from the dealership. It protects buyers of second-hand cars by providing a reference that informs their prospective purchases. It's kind of like a CarFax! 🦊 

This PySpark job mimic Hadoop's MapReduce functionality to produce a report of a given car's accident history by year and make. (Py)Spark has a significant advantage over traditional MapReduce's in-mem processing performance. 

## The Data
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

