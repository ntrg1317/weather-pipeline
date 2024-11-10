FROM ubuntu:latest
LABEL authors="trangnt"

ENTRYPOINT ["top", "-b"]

FROM apache/airflow:2.10.2-python3.8

COPY requirements.txt ./

RUN pip install -r requirements.txt