FROM python:3.6
WORKDIR /app
ADD requirements.txt .
RUN pip3 install -r requirements.txt
ADD . .