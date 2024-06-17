# Stage 1: Build stage
FROM openjdk:8-jdk-slim AS build

# Stage 2: Production stage
FROM debian:11-slim AS production
# Install necessary packages or perform any additional setup

USER root

RUN apt-get update && \
    apt-get install -y build-essential gcc make bash tini libc6 wget python3 python3-pip

# update pip
RUN pip install --upgrade pip

# Install new packages
RUN pip install delta-spark pyspark==3.5.1 numpy pandas trino minio schedule

# Copy all files from the build stage to the production stage
COPY --from=build /usr/local/openjdk-8 /usr/local/openjdk-8

# Set environment variables
ENV JAVA_HOME=/usr/local/openjdk-8
ENV PATH="$JAVA_HOME/bin:${PATH}"


# Set working directory
WORKDIR /home/jovyan

# Create group jupyter with GID 1000
RUN groupadd -g 1000 jupyter

# Create user jupyter with UID 1000 and GID 1000
RUN useradd -m -s /bin/bash -N -u 1000 -g 1000 jupyter

# Create directories
RUN mkdir -p /home/jupyter/work
RUN mkdir -p /home/jupyter/.jupyter
RUN chown -R jupyter:jupyter /home/jupyter

# Define arguments
ARG NB_GID=1000
ARG NB_UID=1000
ARG NB_USER=jupyter

# Install Jupyter labextension manager and nodejs
RUN pip install jupyter ipywidgets

# Install additional packages
RUN pip install py4j && ln -s /opt/spark/python/pyspark /usr/local/lib/python3.9/dist-packages/pyspark && ln -s /opt/spark/python/pylintrc /usr/local/lib/python3.9/dist-packages/pylintrc
RUN pip install jupyterlab jupyterhub==1.5.0 jupyterlab-horizon-theme JLDracula s3contents && mkdir -p /home/public && chmod 777 /home/public

# Configure Spark
WORKDIR /opt/spark/work-dir
RUN chmod g+w /opt/spark/work-dir
ENV SPARK_HOME=/opt/spark

# Coppy new spark to /opt/spark and entrypoint.sh
COPY /tmp/spark-3.5.1-bin-hadoop3 /opt/spark
COPY /tmp/spark-3.5.1-bin-hadoop3/kubernetes/dockerfiles/spark/entrypoint.sh /opt

COPY /tmp/envSpark /root/.ivy2

RUN mkdir -p /app
WORKDIR /app

ENTRYPOINT ["/opt/entrypoint.sh"]

ENV SPARK_HOME /opt/spark
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip
ENV PYSPARK_PYTHON python3
ENV PYSPARK_DRIVER_PYTHON python3
ENV PYSPARK_DRIVER_PYTHON_OPTS "notebook --ip= --port=8888 --allow-root"
ENV PYSPARK_SUBMIT_ARGS "--master local[2] pyspark-shell"
ENV SPARK_OPTS "--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"
ENV SPARK_MASTER_OPTS "--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"




