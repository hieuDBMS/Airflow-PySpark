from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import explode, arrays_zip, from_unixtime
from pyspark.sql.types import DateType
from pyspark.sql.functions import date_format
from datetime import datetime
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import lit
import pyspark.sql.functions as F
import os
import sys

if __name__ == '__main__':
    def app():
        spark = SparkSession.builder.appName("Practice1") \
            .master("spark://spark-master:7077") \
            .config("spark.jars", "/spark/jars/ojdbc8-21.9.0.0.jar") \
            .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio")) \
            .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")) \
            .config("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://host.docker.internal:9000")) \
            .config("fs.s3a.connection.ssl.enabled", "false") \
            .config("fs.s3a.path.style.access", "true") \
            .config("fs.s3a.attempts.maximum", "1") \
            .config("fs.s3a.connection.establish.timeout", "5000") \
            .config("fs.s3a.connection.timeout", "10000") \
            .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") \
            .getOrCreate()

        # Join 2 table
        df_eba_customer = spark.read.parquet(f"s3a://epos-practice/EBA_CUSTOMER")
        df_eba_contract = spark.read.parquet(f"s3a://epos-practice/EBA_CONTRACT")
        current_timestamp = lit(datetime.now()).cast(TimestampType())
        joined_table = df_eba_contract.join(df_eba_customer, on=['CUSTID'], how="left") \
            .fillna({  # fillna only support numeric, string, boolean
            "USERMODIFY": "UNKNOWN"
        }) \
            .withColumn("MODIFYDATE", F.coalesce(F.col("MODIFYDATE"), current_timestamp))

        joined_table = joined_table.select([column for column in joined_table.columns if column not in ["BRID", "CUSTTYPE", "STATUS"]])
        joined_table.show(20, truncate=False)

        # Repartition
        # joined_table.repartition(3)
        num_partitions = joined_table.rdd.getNumPartitions()
        print(f"Number of partitions: {num_partitions}")

        # Store to minio
        joined_table.write \
                    .mode("overwrite") \
                    .parquet(f"s3a://epos-practice/JOIN_TABLES")


    app()
    os.system('kill %d' % os.getpid())
