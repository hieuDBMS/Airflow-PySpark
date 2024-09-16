# Use the official Spark image as the base image
FROM bitnami/spark:latest

USER root

# Install necessary packages
RUN apt-get update && apt-get install -y wget tar python3-pip

# Set environment variables for Hadoop
ENV HADOOP_VERSION=3.4.0
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH

# Create directories for Hadoop installation
RUN mkdir -p $HADOOP_HOME

# Download and install Hadoop
RUN wget https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz -O /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C $HADOOP_HOME --strip-components=1 \
    && rm /tmp/hadoop.tar.gz

# Create the target directory for Spark application code
RUN mkdir -p /opt/bitnami/spark/learnspark

# Copy your application code to the container
COPY ./execute_file.py /opt/bitnami/spark/learnspark/execute_file.py

# Download and move necessary JAR files to Spark's jars directory
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
    && wget https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar \
    && mv hadoop-aws-3.3.2.jar /opt/bitnami/spark/jars/ \
    && mv aws-java-sdk-bundle-1.11.1026.jar /opt/bitnami/spark/jars/ \
    && mv ojdbc8-21.9.0.0.jar /opt/bitnami/spark/jars/

# Install Python dependencies
RUN pip3 install pyspark oracledb

# Set environment variables for MinIO
ENV AWS_ACCESS_KEY_ID=minio
ENV AWS_SECRET_ACCESS_KEY=minio123
ENV ENDPOINT=http://host.docker.internal:9000

# Set the working directory
WORKDIR /opt/bitnami/spark/learnspark

# Set the default command to run the PySpark script
#CMD ["spark-submit", "/opt/bitnami/spark/learnspark/execute_file.py"]
