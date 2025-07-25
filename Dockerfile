FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libxml2-dev libxslt-dev libffi-dev \
    libjpeg-dev zlib1g-dev \
    git curl libgl1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY .env .
COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
