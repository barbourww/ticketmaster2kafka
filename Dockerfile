FROM python:3.12

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .


RUN mkdir -p /var/log/ticketmaster2kafka
CMD ["python3", "ticketmaster2kafka.py"]
