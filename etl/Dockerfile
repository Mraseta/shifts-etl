FROM python:3.12-slim

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY .env .
COPY src/ .

CMD ["python", "etl.py"]