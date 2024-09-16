FROM quay.io/astronomer/astro-runtime:11.7.0
ENV AIRFLOW__CORE__EXECUTOR=CeleryExecutor
ENV AIRFLOW__CORE__TEST_CONNECTION=Enabled