FROM debian:11-slim
USER root

RUN apt-get update && \
    apt-get install -y build-essential gcc make bash tini libc6 wget python3 python3-pip git

# update pip
RUN pip install --upgrade pip

# Install new packages
RUN pip install numpy pandas schedule websocket-client kafka-python kafka
RUN pip install git+https://github.com/dpkp/kafka-python.git
  

# Copy all files from the build stage to the production stage
RUN mkdir -p /app

# Execute the delayed execution script during the build process
WORKDIR /app

USER 1001

# docker build -t duxting404/kafka-producer -f Dockerfile .
# run the container
# docker run -it --rm duxting404/kafka-producer, mount the volume to the container ./data/Kafka:/app
# docker run -it duxting404/kafka-producer



