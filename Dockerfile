FROM apache/airflow:2.10.2-python3.8


# Install OpenJDK-11
#RUN apt-get update && \
#    apt-get install -y openjdk-8-jdk ant && \
#    apt-get clean && \
#    rm -rf /var/lib/apt/lists/*
#
## Set JAVA_HOME
#ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
#ENV PATH $JAVA_HOME/bin:$PATH
#RUN export JAVA_HOME

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME
USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark==2.1.3
COPY ./requirements.txt /
RUN pip install -r /requirements.txt

COPY --chown=airflow:root ./dags /opt/airflow/dags