FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get install -y python3.8 python3-pip
RUN apt-get install -y python3.8-distutils
RUN apt-get install -y openjdk-11-jdk

# Update symlink to point to latest
RUN rm /usr/bin/python3 && ln -s /usr/bin/python3.8 /usr/bin/python3
RUN python3 --version
RUN pip3 --version
RUN java -version
RUN pip install poetry

COPY . /python-deequ
WORKDIR python-deequ

RUN poetry lock --no-update
RUN poetry install
RUN poetry add pyspark==3.3

ENV SPARK_VERSION=3.3
CMD poetry run python -m pytest -s tests
