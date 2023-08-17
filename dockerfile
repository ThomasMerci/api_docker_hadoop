FROM python:3.9-slim

RUN apt-get update && apt-get install -y gcc libffi-dev libssl-dev \
    && pip install --no-cache-dir hdfs \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY api_hadoop.py /api_hadoop.py

CMD ["python", "api_hadoop.py"]
