FROM apache/spark:4.1.0-preview3

USER root

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    wget \
    netcat-openbsd \
    iputils-ping \
    zip \
    unzip \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

WORKDIR /app

RUN mkdir -p /tmp/package/dependencies/src
COPY ../../../dependencies/src/udf_functions.py /tmp/package/dependencies/src/

RUN cd /tmp/package && zip -r /opt/spark/dependencies.zip .

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY ../../../. .

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=/app:$PYTHONPATH

RUN mkdir -p /tmp/spark-warehouse /tmp/spark-local

CMD ["python3", "src/analysis/main.py"]