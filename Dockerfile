FROM apache/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Ensure a writable Ivy cache directory for the 'spark' user
RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt || true

WORKDIR /workspace

USER spark