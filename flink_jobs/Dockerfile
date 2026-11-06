FROM flink:1.20-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install apache-flink==1.20.5 psycopg2-binary