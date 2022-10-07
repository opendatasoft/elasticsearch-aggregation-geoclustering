FROM docker.elastic.co/elasticsearch/elasticsearch:7.17.6 AS elasticsearch-plugin-debug

COPY /build/distributions/geopoint-clustering-aggregation-7.17.6.0.zip /tmp/geopoint-clustering-aggregation-7.17.6.0.zip
RUN ./bin/elasticsearch-plugin install file:/tmp/geopoint-clustering-aggregation-7.17.6.0.zip
