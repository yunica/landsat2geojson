FROM python:3.9.12-slim-buster
RUN apt-get update
RUN python -m pip install --upgrade pip
RUN apt-get install -y git
WORKDIR /script
COPY requirements.txt .
RUN pip install --upgrade --ignore-installed --no-cache-dir -r requirements.txt
COPY . .
RUN python setup.py install
VOLUME /mnt
WORKDIR /mnt