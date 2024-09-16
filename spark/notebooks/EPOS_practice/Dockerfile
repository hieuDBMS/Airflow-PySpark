FROM bde2020/spark-python-template:3.3.0-hadoop3.3

COPY execute_file.py /app/

# Upgrade pip to the latest version

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    wget https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar && \
    wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    mv hadoop-aws-3.3.2.jar /spark/jars/ && \
    mv aws-java-sdk-bundle-1.11.1026.jar /spark/jars/ && \
    mv ojdbc8-21.9.0.0.jar /spark/jars/ && \
    mv postgresql-42.7.3.jar /spark/jars/

ENV AWS_ACCESS_KEY_ID minio
ENV AWS_SECRET_ACCESS_KEY minio123
ENV ENDPOINT http://host.docker.internal:9000
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/execute_file.py
ENV ENABLE_INIT_DAEMON false
