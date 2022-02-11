FROM docker.elastic.co/elasticsearch/elasticsearch-oss:7.10.2 AS elasticsearch-plugin-debug

COPY /build/distributions/geopoint-clustering-aggregation-7.10.2.0.zip /tmp/geopoint-clustering-aggregation-7.10.2.0.zip
RUN ./bin/elasticsearch-plugin install file:/tmp/geopoint-clustering-aggregation-7.10.2.0.zip
