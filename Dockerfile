FROM apache/airflow:2.10.4

USER root

WORKDIR /opt

RUN rm -rf /opt/java11/*
COPY /lib/openlogic-openjdk-11.0.26+4-linux-x64.tar.gz /opt/

RUN tar -xvzf openlogic-openjdk-11.0.26+4-linux-x64.tar.gz && \
    mv openlogic-openjdk-11.0.26+4-linux-x64 java11 && \
    echo 'export JAVA_HOME=/opt/java11' >> /etc/profile && \
    echo 'export PATH=$JAVA_HOME/bin:$PATH' >> /etc/profile

ENV JAVA_HOME=/opt/java11
ENV PATH=$JAVA_HOME/bin:$PATH

CMD ["/bin/bash"]

USER airflow

# Instalar dependências do Python
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Copiar DAGs para o diretório do Airflow
COPY airflow/dags /opt/airflow/dags