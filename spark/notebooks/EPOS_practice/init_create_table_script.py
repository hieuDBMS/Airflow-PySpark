from pyspark.sql import SparkSession
from pyspark.sql.types import *

import os
import json

if __name__ == "__main__":
    # Function to convert Spark data types to Oracle data types
    def spark_to_oracle_type(spark_type):
        if isinstance(spark_type, StringType):
            return "VARCHAR2(255)"
        elif isinstance(spark_type, IntegerType):
            return "NUMBER(10)"
        elif isinstance(spark_type, LongType):
            return "NUMBER(19)"
        elif isinstance(spark_type, DoubleType):
            return "NUMBER(19,4)"
        elif isinstance(spark_type, BooleanType):
            return "NUMBER(1)"
        elif isinstance(spark_type, TimestampType):
            return "TIMESTAMP"
        elif isinstance(spark_type, DateType):
            return "DATE"
        else:
            return "VARCHAR2(255)"

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

        # Read Parquet files from MinIO
        df = spark.read.parquet("s3a://epos-practice/JOIN_TABLES")
        # Get the schema of the Parquet file
        parquet_schema = df.schema
        # Define oracle schema
        schema = "ODI_REPO"
        # Define table name
        table_name = schema+"."+"JOIN_TABLES_TEST"
        # Define create table script
        create_table_script = f"CREATE TABLE {table_name} (\n"
        for field in parquet_schema.fields:
            oracle_type = spark_to_oracle_type(field.dataType)
            create_table_script += f"""       "{field.name}" {oracle_type},\n"""
        create_table_script = create_table_script.rstrip(",\n") + "\n)"

        return table_name, create_table_script

    result = app()

    # Write the script to txt file
    with open('/app/return.json', 'w') as f:
        json.dump(
            {
                "table_name": result[0],
                "create_table_script": result[1]
            },
            f
        )
