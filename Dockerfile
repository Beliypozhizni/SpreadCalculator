FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY src ./src

RUN pip install --no-cache-dir \
    "redis>=7.3.0,<8.0.0" \
    "python-dotenv>=1.2.2,<2.0.0" \
    "easy-logging @ git+https://github.com/Beliypozhizni/EasyLogging.git@v.1.0.0" \
    "spreads-detector-schemas @ git+https://github.com/Beliypozhizni/SpreadsDetectorSchemas.git@v.1.0.0"

CMD ["python", "-m", "src.main"]
