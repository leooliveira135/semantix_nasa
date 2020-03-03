import pandas as pd

df_nasa_jul = pd.read_csv('/home/excript/semantix/NASA_access_log_Jul95',delimiter=' ',header=None,error_bad_lines=False)

df_nasa_jul = df_nasa_jul.drop([1,2],axis=1)
df_nasa_jul

df_nasa_jul[2]=df_nasa_jul[3].map(str)+' '+df_nasa_jul[4].map(str)
df_nasa_jul.drop([3,4],axis=1,inplace=True)
df_nasa_jul

df_nasa_jul = df_nasa_jul[[0,2,5,6,7]]
df_nasa_jul

df_columns = ['host','timestamp','requisicao','code_http','total_bytes']
df_nasa_jul.columns = df_columns
df_nasa_jul['code_http'] = df_nasa_jul['code_http'].fillna(0).astype(int)
df_nasa_jul['timestamp'] = df_nasa_jul['timestamp'].str.strip('[]').astype(str)
df_nasa_jul

df_nasa_aug = pd.read_csv('/home/excript/semantix/NASA_access_log_Aug95',delimiter=' ',header=None,error_bad_lines=False)

df_nasa_aug = df_nasa_aug.drop([1,2],axis=1)
df_nasa_aug

df_nasa_aug[2]=df_nasa_aug[3].map(str)+' '+df_nasa_aug[4].map(str)
df_nasa_aug.drop([3,4],axis=1,inplace=True)
df_nasa_aug

df_nasa_aug = df_nasa_aug[[0,2,5,6,7]]
df_nasa_aug

df_columns = ['host','timestamp','requisicao','code_http','total_bytes']
df_nasa_aug.columns = df_columns
df_nasa_aug['code_http'] = df_nasa_aug['code_http'].fillna(0).astype(int)
df_nasa_aug['timestamp'] = df_nasa_aug['timestamp'].str.strip('[]').astype(str)
df_nasa_aug

df_nasa = pd.concat([df_nasa_jul,df_nasa_aug],ignore_index=True)
df_nasa

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

spark = SparkSession.builder\
		.master("local[4]")\
		.appName("NASA Kennedy Space Center WWW server")\
		.config("spark.sql.execution.arrow.enabled", "true")\
		.config("spark.memory.fraction", 0.8)\
		.config("spark.executor.memory", "1g")\
		.config("spark.driver.memory", "1g")\
		.getOrCreate()

nasaSchema = StructType([StructField("host",StringType(),True),\
	     StructField("timestamp",StringType(),True),\
	     StructField("requisicao",StringType(),True),\
	     StructField("code_http",IntegerType(),True),\
	     StructField("total_bytes",StringType(),True)])

nasa = spark.createDataFrame(df_nasa,schema=nasaSchema)

nasa.select('host').distinct().count().show()

nasa.filter(f.col('code_http')==404).count().show()

nasa.groupBy("requisicao").agg(f.count('requisicao').alias('qtd')).sort(f.desc(qtd)).limit(5).show()

nasa.withColumn('data',nasa['timestamp'].substr(1,10)).show()

nasa.filter(f.col('code_http')==404).groupBy('data').agg(f.count('code_http')).show()

nasa.filter(f.col('code_http')==404).groupBy('data').agg(f.sum('total_bytes')).show()

