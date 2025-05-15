#FROM python:3.9-slim
#
#WORKDIR /app
#
#COPY jobs/ /app/
#COPY requirements.txt .
#
#RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt
#
#
#CMD ["python", "main.py"]

#
#FROM bitnami/spark:latest
#
#WORKDIR /opt/bitnami/spark/jobs
#
#COPY . .
#
## Install Python dependencies for `main.py`
#RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt
#
## Default command (used by data-producer)
#CMD ["python", "main.py"]


FROM python:3.9-slim

WORKDIR /app

COPY . .
RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
