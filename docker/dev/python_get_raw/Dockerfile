FROM python:3.8-slim

WORKDIR /app

COPY ./requirements.txt ./requirements.txt

RUN pip install -r requirements.txt

COPY ./app/main.py /app/main.py

CMD ["python", "./main.py"]