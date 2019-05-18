FROM adoptopenjdk/openjdk11:latest as download

ARG ACTIVEMQ_VERSION=5.15.9

RUN cd /tmp && \
    curl -o apache-activemq-${ACTIVEMQ_VERSION}-bin.tar.gz https://archive.apache.org/dist/activemq/${ACTIVEMQ_VERSION}/apache-activemq-${ACTIVEMQ_VERSION}-bin.tar.gz && \
    tar -xf apache-activemq-${ACTIVEMQ_VERSION}-bin.tar.gz && \
    mv apache-activemq-${ACTIVEMQ_VERSION} apache-activemq


FROM adoptopenjdk/openjdk11:latest

ARG ACTIVEMQ_VERSION=5.15.9

LABEL activemq.version=${ACTIVEMQ_VERSION}

COPY --from=download /tmp/apache-activemq/ /usr/local/apache-activemq/
COPY activemq.xml /usr/local/apache-activemq/conf/activemq.xml

EXPOSE 61616
EXPOSE 5672
EXPOSE 61613
EXPOSE 1883
EXPOSE 61614

CMD [ "/usr/local/apache-activemq/bin/activemq", "console" ]
