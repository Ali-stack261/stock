from pyspark.sql import SparkSession
import pyspark.sql.group as g
import pyspark.sql.dataframe as df

print('GroupedData methods:', [m for m in dir(g.GroupedData) if 'State' in m or 'state' in m])
print('DataFrame methods:', [m for m in dir(df.DataFrame) if 'State' in m or 'state' in m])
