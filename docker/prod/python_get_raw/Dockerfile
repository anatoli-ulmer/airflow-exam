FROM python:3.8-slim

WORKDIR /app

COPY ./requirements.txt ./requirements.txt

RUN pip install -r requirements.txt

COPY ./main.py ./main.py

RUN mkdir -p raw_files