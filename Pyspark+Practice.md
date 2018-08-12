

```python
#sc.stop()
from pyspark import SparkConf,SparkContext
conf = SparkConf().setAppName('hello').setMaster('local[2]')
sc = SparkContext(conf=conf)
```


```python
# Json data load for Spark 1.6.2 version
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df_json = sqlContext.read.json("D:\\Pyspark_practice\\order_json.json")
```


```python
# To query the dataset
df_json.registerTempTable("order")
df = sqlContext.sql("select * from order where category_id  between 3 and 9")
df.show()
```


```python
#save the data in json format
df.write.json("D:\\Pyspark_practice\\output_json")
df.write.save("D:\\Pyspark_practice\\output1_json","json")
```


```python
#save the data in csv or any delimiter format
#for higher than 2.0 version
df.write.csv(sep="|",path="D:\\Pyspark_practice\\output2",header=True)
```


```python
#for all version # save it with different delimeter
df.coalesce(1).write.format('com.databricks.spark.csv').save("D:\\Pyspark_practice\\order2_text",header = True,sep="|")

```


```python
# load csv or any delimeter format data
# with header/column name
#for version spark 2.x

#df_csv = sqlContext.read.csv("D:\\Pyspark_practice\\input_csv\\customer_header.csv",header=True)
#df_txt = sqlContext.read.csv("D:\\Pyspark_practice\\input_csv\\customer_header.txt",header=True,sep="|")



```


```python
# load csv or any delimeter format data
# with header/column name
#for version spark 1.6.x
df_csv = sqlContext.read.format('com.databricks.spark.csv').options(header=True, inferschema=True).\
load('D:\\Pyspark_practice\\input_csv\\customer_header.csv')

df_txt = sqlContext.read.format('com.databricks.spark.csv').options(header=True,inferschema=True).options(delimiter='|').\
load('D:\\Pyspark_practice\\input_csv\\customer_header.txt')

```


```python
# load csv or any delimeter format data
# without header/column name
#for version spark 2.x
from pyspark.sql.types import *

RDD_load = sc.textFile("D:\Pyspark_practice\input_csv\customer.txt")
RDD_map = RDD_load.map(lambda s:(s.split(",")[0],s.split(",")[1],s.split(",")[2],s.split(",")[3],s.split(",")[4],
                                 s.split(",")[5]))
schema = StructType([StructField('Fname',StringType(),True),StructField('Lname',StringType(),True),
                    StructField('city',StringType(),True),StructField('Employer',StringType(),True),
                    StructField('Ph',StringType(),True),StructField('Email',StringType(),True)])

#df_csv = sqlContext.createDataFrame(RDD_map,schema)
df_csv = RDD_map.toDF(schema)

```


```python
df_csv.registerTempTable('customer')
df = sqlContext.sql('select * from customer where Ph !="" ')
df.show()
```
