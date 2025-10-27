FROM bitnami/spark:3.4.3

# JAR kopieren
COPY target/log-analysis-csv-1.0-jar-with-dependencies.jar /opt/bitnami/spark/log-analysis.jar

USER root
RUN useradd -ms /bin/bash -u 1001 sparkuser && \
    chown -R sparkuser:sparkuser /opt/bitnami/spark
USER sparkuser

WORKDIR /opt/bitnami/spark
