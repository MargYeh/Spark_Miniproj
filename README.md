# Spark_Miniproj
This project uses Spark transformations to solve traditional MapReduce problems. It takes in data related to information and incidents of multiple automobiles after they are sold and outputs a report that contains the number of accidents attributed to each vehicle. The raw data is stored in the form of a CSV in HDFS and contains the following schema:
| Column  	| Type 						|
| ------------- 	| ------------- 					|
| incident_id  	| INT  						|
| incident_type 	| STRING (I: initial sale, A: accident, R: Repair)  	|
| vin_number 	| STRING 					|
| make	 	| STRING (The brand of the car, only populated with incident type ‘I’|
| model 	| STRING (The model of the car, only populated with incident type ‘I’|
| year		| STRING (The year of the car, only populated with incident type ‘I’|
| incident_date	| DATE (Date of the incident occurrence	|
| description	| STRING 	|

## How to run:
Set up Oracle Virtualbox with an OVA of HDP Sandbox:

Download links for HDP Sandbox on VirtualBox, use one to set it up:
- HDP 2.5.0 (https://archive.cloudera.com/hwx-sandbox/hdp/hdp-2.5.0/HDP_2.5_virtualbox.ova)
- HDP 2.6.5 (https://archive.cloudera.com/hwx-sandbox/hdp/hdp-2.6.5/HDP_2.6.5_virtualbox_180626.ova)
- HDP 3.0.1 (https://archive.cloudera.com/hwx-sandbox/hdp/hdp-3.0.1/HDP_3.0.1_virtualbox_181205.ova)

Within the shell, use the following commands:
Load in Spark_miniproj.py using:
```
wget https://raw.githubusercontent.com/MargYeh/Spark_Miniproj/main/Spark_miniproj.py
```
Or use the following to upload from local:
```
scp -P 2222 -oHostKeyAlgorithms=+ssh-dss Spark_miniproj.py root@127.0.0.1:/root
```

![image](https://github.com/user-attachments/assets/4b34ba65-274a-4021-9c8f-3e8054482785)

Upload data.csv to /user/root in HDFS:
![image](https://github.com/user-attachments/assets/81f69278-d13d-47c8-bb2f-da501e8d0d0c)

In the shell, run with: 
```
spark-submit Spark_miniproj.py
```
![image](https://github.com/user-attachments/assets/f484552e-320c-489a-8d29-434f09e6b420)

## Results:
The following should be printed to the terminal 
![image](https://github.com/user-attachments/assets/613c8431-e02c-46d5-a9e8-8f62d53430d9)

The output of results.txt can be shown as below:
![image](https://github.com/user-attachments/assets/30b214d8-b4f6-4196-b38b-1cd717111488)

