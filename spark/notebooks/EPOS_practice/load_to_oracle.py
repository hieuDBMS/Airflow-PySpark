from pyspark.sql import SparkSession
import os

if __name__ == '__main__':
    def app():
        # Initialize Spark session
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
        print(df.dtypes)
        # Define Oracle parameters
        oracle_url = "10.86.108.227:1521:orcl"
        oracle_properties = {
            "user": "EPOS",
            "password": "admin",
        }

        # Define the table name
        table_name = "ODI_REPO.JOIN_TABLES_TEST"

        # Write DataFrame to Oracle table
        df.write \
            .jdbc(url=f"jdbc:oracle:thin:@{oracle_url}", table=table_name, mode="append", properties={
            "user": oracle_properties["user"],
            "password": oracle_properties["password"],
            "driver": "oracle.jdbc.OracleDriver"
        })

        # Stop the Spark session
        spark.stop()


    app()
