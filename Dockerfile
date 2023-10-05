FROM continuumio/miniconda
MAINTAINER Oleg Koval <oleh.koval@nixsolutions.com>

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=/opt/conda/envs/my_venv/bin:$PATH

WORKDIR /project

COPY . /project

RUN sed -i 's/buster/oldstable/g' /etc/apt/sources.list && \
    apt-get update && \
    apt-get -y upgrade && \
    conda install -c anaconda openjdk && \
    mkdir -p /usr/share/man/man1 && \
    apt-get -y install openjdk-11-jdk && \
    find . -name \*.pyc -delete && \
    conda create -n my_venv python=3.7.9 && \
    echo /bin/bash -c "source activate my_venv" && \
    pip install --no-cache-dir -r requirements.txt
