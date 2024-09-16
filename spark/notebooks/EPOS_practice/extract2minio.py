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

        # Connect to Oracle EBA_CONTRACT table
        df_eba_contract = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:oracle:thin:@//10.86.108.227:1521/orcl") \
            .option("dbtable", "EPOS.EBA_CONTRACT") \
            .option("user", "EPOS") \
            .option("password", "admin") \
            .load()

        # Connect to Oracle EBA_CUSTOMER table
        df_eba_customer = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:oracle:thin:@//10.86.108.227:1521/orcl") \
            .option("dbtable", "EPOS.EBA_CUSTOMER") \
            .option("user", "EPOS") \
            .option("password", "admin") \
            .load()

        # Show infor of 2 tables
        df_eba_customer.show(10, truncate=False)
        print(f"The total rows of EBA_CUSTOMER: {df_eba_customer.count()}")
        df_eba_contract.show(10, truncate=False)
        print(f"The total rows of EBA_CONTRACT: {df_eba_contract.count()}")

        # Store in Minio
        df_eba_contract.write \
            .mode("overwrite") \
            .parquet(f"s3a://epos-practice/EBA_CONTRACT")

        df_eba_customer.write \
            .mode("overwrite") \
            .parquet(f"s3a://epos-practice/EBA_CUSTOMER")


    app()
    os.system('kill %d' % os.getpid())
