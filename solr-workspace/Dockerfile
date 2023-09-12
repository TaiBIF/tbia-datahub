FROM solr:8.11.1

USER root
WORKDIR /workspace
COPY ./jts-core-1.18.1.jar /opt/solr-8.11.1/server/solr-webapp/webapp/WEB-INF/lib

USER solr

# RUN precreate-core
# WORKDIR /opt/solr-8.11.1
