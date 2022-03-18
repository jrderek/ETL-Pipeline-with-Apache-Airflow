FROM ubuntu:20.04

## Never prompt user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive

## update
RUN apt-get update
RUN apt-get upgrade
RUN apt-get install -y locales \
    build-essential \
    wget \
    unzip \
    curl \
    nano \
    openssh-server

## Define en_US.
RUN locale-gen en_US.UTF-8
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

## set variables & upate PATH
ENV PYTHON_HOME /opt/anaconda3
ENV AIRFLOW_VERSION 1.10.10
ENV AIRFLOW_HOME /opt/airflow
ENV AIRFLOW_CONFIG ${AIRFLOW_HOME}/airflow.cfg
ENV PATH="${PYTHON_HOME}/bin:${PATH}"

## install miniconda from source
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /opt/miniconda.sh
RUN bash /opt/miniconda.sh -b -p /opt/anaconda3
RUN conda install -y python=3.8.3
RUN conda --version
RUN python --version
RUN which python

## install airflow
RUN conda install -c conda-forge -y psycopg2 airflow==${AIRFLOW_VERSION} boto3 awscli sshtunnel paramiko pyspark
COPY config/airflow.cfg /opt/airflow/airflow.cfg
COPY config/airflow_variables.json /opt/airflow/airflow_variables.json
COPY config/aws-ssh-key.pem /opt/aws-ssh-key.pem
COPY scripts/ /opt/scripts/
COPY python/ /opt/python/
COPY resources/ /opt/resources/

## change permissions
RUN chmod +x /opt/scripts/docker/entrypoint.sh

## expose ports
EXPOSE 8080 5555 8793

## define entrypoint
ENTRYPOINT ["/opt/scripts/docker/entrypoint.sh"]
