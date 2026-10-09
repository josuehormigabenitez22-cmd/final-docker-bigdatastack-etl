# Matenemos simple el dockerfile
# Para ello le metemos una imagen poco pesada y en este caso, metemos tambien el código sin montarlo como volumen
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl/ ./etl/
COPY data/ ./data/

CMD ["python", "etl/pipeline.py"]