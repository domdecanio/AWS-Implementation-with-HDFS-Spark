# Using The "my_pyspark_app" Application

The first task that must be completed to use this software is correct setup of a HDFS cluster running Apache Spark on an AWS EC2 instance. After completing this step, follow the instructions below.

- 1. Load the data (export.csv) into your master ec2 instance in the root directory, and pull this file into your hadoop cluster
- 2. Load the application into your master ec2 instance in the root directory
- 3. Run the following command in your master ec2 instance terminal : cd spark-3.3.1-bin-hadoop3
- 4. Run the following command in your master ec2 instance terminal :bin/spark-submit /home/ubuntu/my_pyspark_app.py "export.csv" "answer.csv"

You may then run the following command in your master ec2 instance to verify that the application executed successfully:
hdfs dfs -ls hdfs://<ec2_private_ip>:9000/