FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    xvfb \
    libnss3 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    fonts-liberation \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install chromium

COPY iderma_pipeline.py .

RUN mkdir -p /data

CMD ["sh", "-c", "echo '>>> CMD ARRANCADO <<<'; xvfb-run -a python -u iderma_pipeline.py"]
