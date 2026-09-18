FROM python:3.11-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim AS runtime

COPY --from=builder /install /usr/local

WORKDIR /app
COPY shared/ ./shared/
COPY consumers/stats/ ./consumers/stats/

ENV PYTHONUNBUFFERED=1
WORKDIR /app/consumers/stats

EXPOSE 8002
CMD ["python", "main.py"]
