FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

# Dependencies
RUN apt-get update && apt-get install -y \
    libiodbc2-dev \
    libiodbc2 \
    iodbc \
    python3-pip \
    python3 \
    unzip \
    wget && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install pandas pyarrow

# Make the dir of the app
WORKDIR /app

# Make a copy of the driver
COPY ODBC2024LINUX64PACK290089j.zip driver.zip
# Unzip and install the driver
RUN unzip driver.zip && \
    chmod +x *.so && \
    mv *.so /usr/lib/x86_64-linux-gnu/ && \
    echo "[HFSQL]" > /etc/odbcinst.ini && \
    echo "Description = HFSQL ODBC Driver" >> /etc/odbcinst.ini && \
    echo "Driver = /usr/lib/x86_64-linux-gnu/wd290hfo64.so" >> /etc/odbcinst.ini && \
    rm driver.zip

# It copies the DSN
COPY odbc.ini /root/.odbc.ini

COPY pyproject.toml .

COPY src ./src

ENV PYTHONPATH=/app/src

CMD ["python3", "src/hfsql_guide/linux/hfsql.py"]