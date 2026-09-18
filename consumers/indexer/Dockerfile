FROM python:3.11-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim AS runtime

COPY --from=builder /install /usr/local

WORKDIR /app
COPY shared/ ./shared/
COPY consumers/indexer/ ./consumers/indexer/

ENV PYTHONUNBUFFERED=1
WORKDIR /app/consumers/indexer

EXPOSE 8001
CMD ["python", "main.py"]
