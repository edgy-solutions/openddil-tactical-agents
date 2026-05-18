FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y libsnappy-dev gcc g++ && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "faust-streaming>=0.10.0" \
    "aiokafka<0.12.0" \
    python-snappy \
    pyyaml \
    "protobuf>=6.30.0,<7.0.0"

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "/app/faust_regional.py"]
