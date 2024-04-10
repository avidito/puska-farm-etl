FROM python:3.8

WORKDIR /scripts

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY etl/ /scripts/etl