FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/vendor:/app

WORKDIR /app

COPY src ./src
COPY vendor ./vendor

CMD ["python", "-m", "src.main"]
