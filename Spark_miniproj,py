#%%
from pyspark import SparkContext
from operator import add

# Sample data 
#1,I,VXIO456XLBB630221,Nissan,Altima,2003,2002-05-08,Initial sales from TechMotors

def extract_vin_key_value(line:str):
    input = line.split(',')
    vin_number = input[2]
    make = input[3]
    year = input[5]
    incident = input[1]
    value = (make, year, incident)
    result = (vin_number, value)
    return result

def populate_make(values):
    verified = []
    for value in values:
        if value[0] != '' and value[1] != '':
            mastermake = value[0]
            masteryear = value[1]
        verified.append((mastermake, masteryear, value[2]))
    return verified

def extract_make_key_value(entry):
    make = entry[0]
    year = entry[1]
    if entry[2] == 'A':
        return (make + '-' + year, 1)
    else:
        return (make + '-' + year, 0)


#Read in input data from CSV file
sc = SparkContext("local", "My Application") 
raw_rdd = sc.textFile("data.csv")

#Perform map operation
vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x)) # Please implement method extract_vin_key_value()

#Group aggregation
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))

#Count number of accidents for the vehicle make and year
make_kv = enhance_make.map(lambda x: extract_make_key_value(x))

#Aggregate key and count number of records in total per key
make_kv_count = make_kv.reduceByKey(add).collect()
print(*make_kv_count, sep = '\n')

#%%
#Save result to HDFS as text
with open('results.txt', 'w') as f:
    for line in make_kv_count:
        f.write(f'{str(line).strip("()")}\n')

#%%
sc.stop()

