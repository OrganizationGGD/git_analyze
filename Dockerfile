FROM python:3.11-slim

RUN groupadd -r gauser && useradd -r -g gauser gauser

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data && chown -R gauser:gauser /app

USER gauser

CMD ["python", "src/main.py"]
