FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app
COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x entrypoint.sh
ENV PYTHONPATH=/app

CMD ["./entrypoint.sh"]
