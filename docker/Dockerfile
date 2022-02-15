FROM docker.elastic.co/elasticsearch/elasticsearch:7.16.3 AS elasticsearch-plugin-debug

COPY /build/distributions/geopoint-clustering-aggregation-7.16.3.0.zip /tmp/geopoint-clustering-aggregation-7.16.3.0.zip
RUN ./bin/elasticsearch-plugin install file:/tmp/geopoint-clustering-aggregation-7.16.3.0.zip
