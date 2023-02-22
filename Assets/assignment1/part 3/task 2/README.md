# Using The "spark_pagerank_app1" Application

The first task that must be completed to use this software is correct setup of a HDFS cluster running Apache Spark on an AWS EC2 instance. After completing this step, follow the instructions below.

- 1. Load the data (web-BerkStan.txt) into your master ec2 instance in the root directory, and pull this file into your hadoop cluster
- 2. Load the application into your master ec2 instance in the root directory
- 3. Run the following command in your master ec2 instance terminal : cd spark-3.3.1-bin-hadoop3
- 4. Run the following command in your master ec2 instance terminal :bin/spark-submit /home/ubuntu/spark_pagerank_app1.py 'web-BerkStan.txt' 'task1_output.txt' 10

You may then enter the ec2 instance's public DNS ip address in your browser followed by :8080 and/or :4040 to verify the application is running.